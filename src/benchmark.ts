import fs from "node:fs";
import { Bench } from "tinybench";
import Table from "cli-table3";
import {
  decode as decodeMsgpack,
  encode as encodeMsgpack,
} from "@msgpack/msgpack";
import { decode as decodeCbor, encode as encodeCbor } from "cbor-x";
import {
  deserialize as deserializeBson,
  serialize as serializeBson,
} from "bson";
import {
  createSessionEncoder,
  decode,
  decodeDirect,
  decodeToCompactJson,
  decodeToTransportJson,
  encode,
  encodeBatch,
  encodeBatchCompact,
  encodeBatchCompactJson,
  encodeBatchDirect,
  encodeBatchTransportJson,
  encodeCompact,
  encodeCompactJson,
  encodeDirect,
  encodeTransportJson,
  init,
  toCompactJson,
  toCompactJsonBatch,
  toTransportJson,
  toTransportJsonBatch,
  type Schema,
  type TwilicValue,
} from "@twilic/core/advanced";
import {
  encodeAvroStream,
  encodeProtobufStream,
  makeSchemaUserRecordBatch,
} from "./schema-codecs.js";

type BackendKind = "napi" | "wasm";
type BenchMode = "full" | "max";

interface CliOptions {
  backend: BackendKind;
  timeMs: number;
  warmupMs: number;
  mode: BenchMode;
  twilicVsMsgpackOnly: boolean;
  markdownOut: string | null;
  jsonOut: string | null;
}

function parseCliOptions(argv: string[]): CliOptions {
  const defaults: CliOptions = {
    backend: "napi",
    timeMs: 1000,
    warmupMs: 250,
    mode: "full",
    twilicVsMsgpackOnly: false,
    markdownOut: null,
    jsonOut: null,
  };

  const options = { ...defaults };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--backend" && argv[i + 1]) {
      const backend = argv[i + 1];
      if (backend === "napi" || backend === "wasm") {
        options.backend = backend;
      }
      i += 1;
      continue;
    }

    if (arg === "--time-ms" && argv[i + 1]) {
      const parsed = Number(argv[i + 1]);
      if (Number.isFinite(parsed) && parsed > 0) {
        options.timeMs = parsed;
      }
      i += 1;
      continue;
    }

    if (arg === "--warmup-ms" && argv[i + 1]) {
      const parsed = Number(argv[i + 1]);
      if (Number.isFinite(parsed) && parsed >= 0) {
        options.warmupMs = parsed;
      }
      i += 1;
      continue;
    }

    if (arg === "--mode" && argv[i + 1]) {
      const mode = argv[i + 1];
      if (mode === "full" || mode === "max") {
        options.mode = mode;
      }
      i += 1;
      continue;
    }

    if (arg === "--twilic-vs-msgpack-only") {
      options.twilicVsMsgpackOnly = true;
    }

    if (arg === "--markdown-out" && argv[i + 1]) {
      options.markdownOut = argv[i + 1];
      i += 1;
      continue;
    }

    if (arg === "--json-out" && argv[i + 1]) {
      options.jsonOut = argv[i + 1];
      i += 1;
      continue;
    }
  }

  return options;
}

interface Dataset {
  singleSmall: TwilicValue;
  singleSmallJson: Record<string, unknown>;
  batchHomogeneous: TwilicValue[];
  batchHomogeneousJson: unknown[];
  batchMixed: TwilicValue[];
  batchMixedJson: unknown[];
  patchSession: {
    first: TwilicValue;
    nextA: TwilicValue;
    nextB: TwilicValue;
    firstTransport: string;
    nextATransport: string;
    nextBTransport: string;
    firstCompact: string;
    nextACompact: string;
    nextBCompact: string;
  };
}

type BenchmarkTransportValue =
  | { t: "null" }
  | { t: "bool"; v: boolean }
  | { t: "i64"; v: string }
  | { t: "u64"; v: string }
  | { t: "f64"; v: number }
  | { t: "string"; v: string }
  | { t: "binary"; v: string }
  | { t: "array"; v: BenchmarkTransportValue[] }
  | { t: "map"; v: Array<[string, BenchmarkTransportValue]> };

type BenchmarkCompactValue = readonly [number] | readonly [number, unknown];

function formatOps(ops: number): string {
  return Math.round(ops).toLocaleString();
}

function formatNsPerOp(ops: number): string {
  if (ops <= 0) {
    return "n/a";
  }

  const ns = 1e9 / ops;
  return Math.round(ns).toLocaleString();
}

function formatBytes(bytes: number): string {
  return bytes.toLocaleString();
}

function formatPercent(value: number): string {
  return `${value.toFixed(2)}%`;
}

function formatReduction(smaller: number, larger: number): string {
  if (smaller <= 0 || larger <= 0) {
    return "n/a";
  }

  return formatPercent((1 - smaller / larger) * 100);
}

function benchmarkToTransportValue(
  value: TwilicValue,
): BenchmarkTransportValue {
  if (value === null) {
    return { t: "null" };
  }
  if (typeof value === "boolean") {
    return { t: "bool", v: value };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("number values must be finite");
    }
    if (Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        throw new Error(
          "unsafe integer number detected; use bigint for 64-bit integers",
        );
      }
      return value >= 0
        ? { t: "u64", v: String(value) }
        : { t: "i64", v: String(value) };
    }
    return { t: "f64", v: value };
  }
  if (typeof value === "bigint") {
    return value >= 0n
      ? { t: "u64", v: value.toString() }
      : { t: "i64", v: value.toString() };
  }
  if (typeof value === "string") {
    return { t: "string", v: value };
  }
  if (value instanceof Uint8Array) {
    return { t: "binary", v: Buffer.from(value).toString("base64") };
  }
  if (Array.isArray(value)) {
    const out: BenchmarkTransportValue[] = Array.from({ length: value.length });
    for (let index = 0; index < value.length; index += 1) {
      out[index] = benchmarkToTransportValue(value[index]);
    }
    return { t: "array", v: out };
  }
  if (typeof value !== "object" || value === null) {
    throw new Error("unsupported value type");
  }

  const entries: Array<[string, BenchmarkTransportValue]> = [];
  const objectValue = value as Record<string, TwilicValue>;
  for (const key of Object.keys(objectValue)) {
    entries.push([key, benchmarkToTransportValue(objectValue[key])]);
  }
  return { t: "map", v: entries };
}

function benchmarkToTransportValues(
  values: TwilicValue[],
): BenchmarkTransportValue[] {
  const out: BenchmarkTransportValue[] = Array.from({ length: values.length });
  for (let index = 0; index < values.length; index += 1) {
    out[index] = benchmarkToTransportValue(values[index]);
  }
  return out;
}

function benchmarkToCompactValue(value: TwilicValue): BenchmarkCompactValue {
  if (value === null) {
    return [0];
  }
  if (typeof value === "boolean") {
    return [1, value];
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("number values must be finite");
    }
    if (Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        throw new Error(
          "unsafe integer number detected; use bigint for 64-bit integers",
        );
      }
      return value >= 0 ? [3, String(value)] : [2, String(value)];
    }
    return [4, value];
  }
  if (typeof value === "bigint") {
    return value >= 0n ? [3, value.toString()] : [2, value.toString()];
  }
  if (typeof value === "string") {
    return [5, value];
  }
  if (value instanceof Uint8Array) {
    return [6, Buffer.from(value).toString("base64")];
  }
  if (Array.isArray(value)) {
    const out: BenchmarkCompactValue[] = Array.from({ length: value.length });
    for (let index = 0; index < value.length; index += 1) {
      out[index] = benchmarkToCompactValue(value[index]);
    }
    return [7, out];
  }
  if (typeof value !== "object" || value === null) {
    throw new Error("unsupported value type");
  }

  const objectValue = value as Record<string, TwilicValue>;
  const keys = Object.keys(objectValue);
  const flat: unknown[] = Array.from({ length: keys.length * 2 });
  for (let index = 0; index < keys.length; index += 1) {
    flat[index * 2] = keys[index];
    flat[index * 2 + 1] = benchmarkToCompactValue(objectValue[keys[index]]);
  }
  return [8, flat];
}

function benchmarkSerializeCompact(value: TwilicValue): string {
  return JSON.stringify(benchmarkToCompactValue(value));
}

function benchmarkSerializeCompactBatch(values: TwilicValue[]): string {
  const out: BenchmarkCompactValue[] = Array.from({ length: values.length });
  for (let index = 0; index < values.length; index += 1) {
    out[index] = benchmarkToCompactValue(values[index]);
  }
  return JSON.stringify(out);
}

function makeSingleSmallDataset(): TwilicValue {
  return {
    id: 1234,
    userId: 987654,
    name: "alice",
    active: true,
    score: 98.5,
    tags: ["edge", "premium", "ap-northeast-1"],
    profile: {
      country: "JP",
      locale: "ja-JP",
      timeZone: "Asia/Tokyo",
    },
  };
}

function makeBatchHomogeneousDataset(): TwilicValue[] {
  return Array.from({ length: 256 }, (_, index) => {
    const id = index + 1;
    return {
      id,
      userId: 100000 + id,
      active: id % 2 === 0,
      tier: id % 3 === 0 ? "gold" : "standard",
      country: id % 5 === 0 ? "US" : "JP",
      usage: {
        requests: 5000 + id,
        errors: id % 17,
      },
    };
  });
}

function makeBatchMixedDataset(): TwilicValue[] {
  return Array.from({ length: 256 }, (_, index) => {
    const id = index + 1;
    const kind = index % 4;
    if (kind === 0) {
      const value: TwilicValue = {
        id,
        tenant: `tenant-${(id % 11) + 1}`,
        active: id % 2 === 0,
        metrics: {
          requests: 50000 + id * 3,
          errors: id % 19,
          latencyMs: 12.5 + (id % 7),
        },
        tags: [`region-${id % 3}`, `tier-${id % 5}`],
      };
      return value;
    }
    if (kind === 1) {
      const value: TwilicValue = {
        id,
        user: {
          name: `user-${id}`,
          age: 20 + (id % 30),
          roles: id % 2 === 0 ? ["admin", "editor"] : ["viewer"],
        },
        active: id % 3 !== 0,
        notes: id % 5 === 0 ? null : `note-${id}`,
      };
      return value;
    }
    if (kind === 2) {
      const value: TwilicValue = {
        id,
        flags: [id % 2 === 0, id % 3 === 0, id % 5 === 0],
        payload: {
          count: 1000 + id,
          checksum: `sha-${(id * 17).toString(16)}`,
          nested: {
            alpha: id % 7,
            beta: id % 11,
          },
        },
      };
      return value;
    }
    const value: TwilicValue = {
      id,
      event: `event-${id % 13}`,
      source: `source-${id % 9}`,
      attrs: {
        region: id % 2 === 0 ? "ap-northeast-1" : "us-east-1",
        plan: id % 3 === 0 ? "pro" : "basic",
        score: 1000 + id,
      },
    };
    return value;
  });
}

function buildDataset(): Dataset {
  const singleSmall = makeSingleSmallDataset();
  const batchHomogeneous = makeBatchHomogeneousDataset();
  const batchMixed = makeBatchMixedDataset();
  const patchSessionFirst: TwilicValue = {
    id: 9001,
    status: "active",
    score: 99.1,
    profile: {
      country: "JP",
      locale: "ja-JP",
      timeZone: "Asia/Tokyo",
    },
  };
  const patchSessionNextA: TwilicValue = {
    id: 9001,
    status: "active",
    score: 99.2,
    profile: {
      country: "JP",
      locale: "ja-JP",
      timeZone: "Asia/Seoul",
    },
  };
  const patchSessionNextB: TwilicValue = {
    id: 9001,
    status: "active",
    score: 99.3,
    profile: {
      country: "JP",
      locale: "ja-JP",
      timeZone: "Asia/Tokyo",
    },
  };

  return {
    singleSmall,
    singleSmallJson: singleSmall as Record<string, unknown>,
    batchHomogeneous,
    batchHomogeneousJson: batchHomogeneous as unknown[],
    batchMixed,
    batchMixedJson: batchMixed as unknown[],
    patchSession: {
      first: patchSessionFirst,
      nextA: patchSessionNextA,
      nextB: patchSessionNextB,
      firstTransport: toTransportJson(patchSessionFirst),
      nextATransport: toTransportJson(patchSessionNextA),
      nextBTransport: toTransportJson(patchSessionNextB),
      firstCompact: toCompactJson(patchSessionFirst),
      nextACompact: toCompactJson(patchSessionNextA),
      nextBCompact: toCompactJson(patchSessionNextB),
    },
  };
}

function formatRelativeSpeed(hz: number, fastestHz: number): string {
  if (hz <= 0 || fastestHz <= 0) {
    return "n/a";
  }

  return `${(hz / fastestHz).toFixed(2)}x`;
}

function escapeMarkdownCell(value: string): string {
  return value.replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
}

interface SizeRowData {
  payload: string;
  twilic: number;
  msgpack: number;
  cbor: number;
  bson: number;
  json: number;
}

interface SchemaSizeRowData {
  payload: string;
  twilicBound: number;
  protobuf: number;
  avro: number;
}

function buildMarkdownReport(params: {
  runtime: string;
  options: CliOptions;
  includeJsonBaseline: boolean;
  sizeRows: SizeRowData[];
  sortedTasks: { name: string; result?: { hz?: number; rme?: number } }[];
  fastestHz: number;
  schemaSizeRows: SchemaSizeRowData[];
}): string {
  const {
    runtime,
    options,
    includeJsonBaseline,
    sizeRows,
    sortedTasks,
    fastestHz,
    schemaSizeRows,
  } = params;

  const lines: string[] = [
    "## Twilic benchmark",
    "",
    `- **Runtime:** ${runtime}`,
    `- **Backend preference:** ${options.backend}`,
    `- **Mode:** ${options.mode}`,
    `- **Baselines:** ${includeJsonBaseline ? "twilic, msgpack, json" : "twilic, msgpack"}`,
    `- **Time per task:** ${options.timeMs} ms`,
    `- **Warmup per task:** ${options.warmupMs} ms`,
    "",
    "### Encoded size comparison",
    "",
  ];

  const sizeHead = [
    "payload",
    "twilic (bytes)",
    "msgpack (bytes)",
    "cbor (bytes)",
    "bson (bytes)",
  ];
  if (includeJsonBaseline) {
    sizeHead.push("json (bytes)");
  }
  sizeHead.push("vs msgpack", "vs cbor", "vs bson");
  if (includeJsonBaseline) {
    sizeHead.push("vs json");
  }
  lines.push("| " + sizeHead.join(" | ") + " |");
  lines.push("| " + sizeHead.map(() => "---").join(" | ") + " |");

  for (const row of sizeRows) {
    const cells = [
      escapeMarkdownCell(row.payload),
      formatBytes(row.twilic),
      formatBytes(row.msgpack),
      formatBytes(row.cbor),
      formatBytes(row.bson),
    ];
    if (includeJsonBaseline) {
      cells.push(formatBytes(row.json));
    }
    cells.push(
      formatReduction(row.twilic, row.msgpack),
      formatReduction(row.twilic, row.cbor),
      formatReduction(row.twilic, row.bson),
    );
    if (includeJsonBaseline) {
      cells.push(formatReduction(row.twilic, row.json));
    }
    lines.push("| " + cells.join(" | ") + " |");
  }

  lines.push("", "### Schema-shared encoded size comparison", "");
  lines.push(
    "| payload | twilic bound (bytes) | protobuf (bytes) | avro (bytes) | vs protobuf | vs avro |",
  );
  lines.push("| --- | ---: | ---: | ---: | ---: | ---: |");
  for (const row of schemaSizeRows) {
    lines.push(
      "| " +
        [
          row.payload,
          formatBytes(row.twilicBound),
          formatBytes(row.protobuf),
          formatBytes(row.avro),
          formatReduction(row.twilicBound, row.protobuf),
          formatReduction(row.twilicBound, row.avro),
        ].join(" | ") +
        " |",
    );
  }

  lines.push("", "### Throughput (sorted by ops/s)", "", "");
  lines.push("| task | ops/s | ns/op | relative | rme |");
  lines.push("| --- | ---: | ---: | ---: | ---: |");

  for (const task of sortedTasks) {
    const hzI = task.result?.hz ?? 0;
    const rme = task.result?.rme;
    lines.push(
      "| " +
        [
          escapeMarkdownCell(task.name),
          formatOps(hzI),
          formatNsPerOp(hzI),
          formatRelativeSpeed(hzI, fastestHz),
          typeof rme === "number" ? formatPercent(rme) : "n/a",
        ].join(" | ") +
        " |",
    );
  }

  return lines.join("\n");
}

function toJsonBytes(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

async function run(): Promise<void> {
  const options = parseCliOptions(process.argv.slice(2));
  const runtime = await init({ prefer: options.backend });
  const includeJsonBaseline = !options.twilicVsMsgpackOnly;
  const dataset = buildDataset();
  const singleSmallJson = dataset.singleSmallJson;
  const batchHomogeneousJson = dataset.batchHomogeneousJson;
  const batchMixedJson = dataset.batchMixedJson;
  const patchSession = dataset.patchSession;
  const schemaUserRecords = makeSchemaUserRecordBatch();
  const schemaUserRecordsTwilic: TwilicValue[] = [];
  for (const record of schemaUserRecords) {
    const twilicRecord: Record<string, TwilicValue> = {
      id: record.id,
      role: record.role,
      active: record.active,
    };
    if (record.age !== undefined) {
      twilicRecord.age = record.age;
    }
    schemaUserRecordsTwilic.push(twilicRecord);
  }
  const schemaUserRecordSchema: Schema = {
    schemaId: 42,
    name: "UserRecordV1",
    fields: [
      {
        number: 1,
        name: "id",
        logicalType: "u32",
        required: true,
        min: 1,
        max: 10_000_000,
      },
      {
        number: 2,
        name: "role",
        logicalType: "string",
        required: true,
        enumValues: ["viewer", "editor", "admin"],
      },
      {
        number: 3,
        name: "age",
        logicalType: "u8",
        required: false,
        min: 0,
        max: 127,
        defaultValue: 0,
      },
      { number: 4, name: "active", logicalType: "bool", required: true },
    ],
  };
  const encodeTwilicBoundStream = (): Uint8Array[] => {
    const encoder = createSessionEncoder();
    return schemaUserRecordsTwilic.map((record) =>
      encoder.encodeWithSchema(schemaUserRecordSchema, record),
    );
  };

  const twilicEncodedSingle = encode(dataset.singleSmall);
  const twilicEncodedBatchHomogeneous = encodeBatch(dataset.batchHomogeneous);
  const twilicEncodedBatchMixed = encodeBatch(dataset.batchMixed);
  const twilicEncodedSingles = dataset.batchHomogeneous.map((value) =>
    encode(value),
  );
  const twilicTransportSingle = toTransportJson(dataset.singleSmall);
  const twilicTransportBatchHomogeneous = toTransportJsonBatch(
    dataset.batchHomogeneous,
  );
  const twilicTransportBatchMixed = toTransportJsonBatch(dataset.batchMixed);
  const twilicCompactSingle = toCompactJson(dataset.singleSmall);
  const twilicCompactBatchHomogeneous = toCompactJsonBatch(
    dataset.batchHomogeneous,
  );
  const twilicCompactBatchMixed = toCompactJsonBatch(dataset.batchMixed);
  const twilicEncodedSingleRaw = encodeTransportJson(twilicTransportSingle);
  const jsonEncodedSingle = toJsonBytes(singleSmallJson);
  const jsonEncodedBatchHomogeneous = toJsonBytes(batchHomogeneousJson);
  const jsonEncodedBatchMixed = toJsonBytes(batchMixedJson);
  const msgpackEncodedSingle = encodeMsgpack(singleSmallJson);
  const msgpackEncodedBatchHomogeneous = encodeMsgpack(batchHomogeneousJson);
  const msgpackEncodedBatchMixed = encodeMsgpack(batchMixedJson);
  const msgpackEncodedSingles = batchHomogeneousJson.map((value) =>
    encodeMsgpack(value),
  );
  const cborEncodedSingle = encodeCbor(singleSmallJson);
  const cborEncodedBatchHomogeneous = encodeCbor(batchHomogeneousJson);
  const cborEncodedBatchMixed = encodeCbor(batchMixedJson);
  const cborEncodedSingles = batchHomogeneousJson.map((value) =>
    encodeCbor(value),
  );
  const bsonEncodedSingle = serializeBson(
    singleSmallJson as Record<string, unknown>,
  );
  const bsonEncodedBatchHomogeneous = serializeBson({
    records: batchHomogeneousJson,
  });
  const bsonEncodedBatchMixed = serializeBson({ records: batchMixedJson });
  const bsonEncodedSingles = batchHomogeneousJson.map((value) =>
    serializeBson(value as Record<string, unknown>),
  );
  const jsonSingleText = new TextDecoder().decode(jsonEncodedSingle);
  const jsonBatchHomogeneousText = new TextDecoder().decode(
    jsonEncodedBatchHomogeneous,
  );
  const jsonBatchMixedText = new TextDecoder().decode(jsonEncodedBatchMixed);
  const twilicEncodedPatchSessionFirst = encode(patchSession.first);
  const twilicBoundSchemaStream = encodeTwilicBoundStream();
  const protobufSchemaStream = encodeProtobufStream(schemaUserRecords);
  const avroSchemaStream = encodeAvroStream(schemaUserRecords);

  const sessionEncoder = createSessionEncoder({
    enableStatePatch: true,
    enableTemplateBatch: true,
  });
  sessionEncoder.encode(patchSession.first);

  const rawSessionEncoder = createSessionEncoder({
    enableStatePatch: true,
    enableTemplateBatch: true,
  });
  rawSessionEncoder.encodeTransportJson(patchSession.firstTransport);

  const directSessionEncoder = createSessionEncoder({
    enableStatePatch: true,
    enableTemplateBatch: true,
  });
  directSessionEncoder.encodeDirect(patchSession.first);

  const compactSessionEncoder = createSessionEncoder({
    enableStatePatch: true,
    enableTemplateBatch: true,
  });
  compactSessionEncoder.encodeCompact(patchSession.first);

  let patchFlip = false;
  let patchFlipRaw = false;
  let patchFlipDirect = false;
  let patchFlipCompact = false;
  let patchFlipCompactRaw = false;

  const bench = new Bench({
    time: options.timeMs,
    warmupTime: options.warmupMs,
  });

  bench
    .add("js preprocess serializeCompact single-small", () => {
      benchmarkSerializeCompact(dataset.singleSmall);
    })
    .add("js preprocess toTransportValue single-small", () => {
      benchmarkToTransportValue(dataset.singleSmall);
    })
    .add("js preprocess serializeCompactBatch batch-homogeneous-256", () => {
      benchmarkSerializeCompactBatch(dataset.batchHomogeneous);
    })
    .add("js preprocess toTransportValues batch-homogeneous-256", () => {
      benchmarkToTransportValues(dataset.batchHomogeneous);
    })
    .add("js preprocess serializeCompactBatch batch-mixed-256", () => {
      benchmarkSerializeCompactBatch(dataset.batchMixed);
    })
    .add("js preprocess toTransportValues batch-mixed-256", () => {
      benchmarkToTransportValues(dataset.batchMixed);
    });

  if (options.mode === "max") {
    bench
      .add("twilic encode single-small (direct)", () => {
        encodeDirect(dataset.singleSmall);
      })
      .add("twilic encode single-small (raw json)", () => {
        encodeTransportJson(twilicTransportSingle);
      })
      .add("twilic encode single-small (compact)", () => {
        encodeCompact(dataset.singleSmall);
      })
      .add("twilic encode single-small (compact raw)", () => {
        encodeCompactJson(twilicCompactSingle);
      })
      .add("twilic decode single-small (direct)", () => {
        decodeDirect(twilicEncodedSingleRaw);
      })
      .add("twilic decode single-small (raw json)", () => {
        decodeToTransportJson(twilicEncodedSingleRaw);
      })
      .add("twilic decode single-small (compact raw)", () => {
        decodeToCompactJson(twilicEncodedSingleRaw);
      })
      .add("msgpack encode single-small", () => {
        encodeMsgpack(singleSmallJson);
      })
      .add("msgpack decode single-small", () => {
        decodeMsgpack(msgpackEncodedSingle);
      })
      .add("cbor encode single-small", () => {
        encodeCbor(singleSmallJson);
      })
      .add("cbor decode single-small", () => {
        decodeCbor(cborEncodedSingle);
      })
      .add("bson serialize single-small", () => {
        serializeBson(singleSmallJson as Record<string, unknown>);
      })
      .add("bson deserialize single-small", () => {
        deserializeBson(bsonEncodedSingle);
      })
      .add("twilic encode batch-homogeneous-256 (direct)", () => {
        encodeBatchDirect(dataset.batchHomogeneous);
      })
      .add("twilic encode batch-homogeneous-256 (raw json)", () => {
        encodeBatchTransportJson(twilicTransportBatchHomogeneous);
      })
      .add("twilic encode batch-homogeneous-256 (compact)", () => {
        encodeBatchCompact(dataset.batchHomogeneous);
      })
      .add("twilic encode batch-homogeneous-256 (compact raw)", () => {
        encodeBatchCompactJson(twilicCompactBatchHomogeneous);
      })
      .add("msgpack encode batch-homogeneous-256", () => {
        encodeMsgpack(batchHomogeneousJson);
      })
      .add("msgpack decode 256 singles", () => {
        for (const encoded of msgpackEncodedSingles) {
          decodeMsgpack(encoded);
        }
      })
      .add("cbor encode batch-homogeneous-256", () => {
        encodeCbor(batchHomogeneousJson);
      })
      .add("cbor decode 256 singles", () => {
        for (const encoded of cborEncodedSingles) {
          decodeCbor(encoded);
        }
      })
      .add("bson serialize batch-homogeneous-256", () => {
        serializeBson({ records: batchHomogeneousJson });
      })
      .add("bson deserialize 256 singles", () => {
        for (const encoded of bsonEncodedSingles) {
          deserializeBson(encoded);
        }
      })
      .add("twilic encode batch-mixed-256 (direct)", () => {
        encodeBatchDirect(dataset.batchMixed);
      })
      .add("twilic encode batch-mixed-256 (raw json)", () => {
        encodeBatchTransportJson(twilicTransportBatchMixed);
      })
      .add("twilic encode batch-mixed-256 (compact)", () => {
        encodeBatchCompact(dataset.batchMixed);
      })
      .add("twilic encode batch-mixed-256 (compact raw)", () => {
        encodeBatchCompactJson(twilicCompactBatchMixed);
      })
      .add("msgpack encode batch-mixed-256", () => {
        encodeMsgpack(batchMixedJson);
      })
      .add("cbor encode batch-mixed-256", () => {
        encodeCbor(batchMixedJson);
      })
      .add("bson serialize batch-mixed-256", () => {
        serializeBson({ records: batchMixedJson });
      })
      .add("twilic patch session-hot (direct)", () => {
        patchFlipDirect = !patchFlipDirect;
        directSessionEncoder.encodePatchDirect(
          patchFlipDirect ? patchSession.nextA : patchSession.nextB,
        );
      })
      .add("twilic patch session-hot (raw json)", () => {
        patchFlipRaw = !patchFlipRaw;
        rawSessionEncoder.encodePatchTransportJson(
          patchFlipRaw
            ? patchSession.nextATransport
            : patchSession.nextBTransport,
        );
      })
      .add("twilic patch session-hot (compact)", () => {
        patchFlipCompact = !patchFlipCompact;
        compactSessionEncoder.encodePatchCompact(
          patchFlipCompact ? patchSession.nextA : patchSession.nextB,
        );
      })
      .add("twilic patch session-hot (compact raw)", () => {
        patchFlipCompactRaw = !patchFlipCompactRaw;
        compactSessionEncoder.encodePatchCompactJson(
          patchFlipCompactRaw
            ? patchSession.nextACompact
            : patchSession.nextBCompact,
        );
      });
  } else {
    bench
      .add("twilic encode single-small", () => {
        encode(dataset.singleSmall);
      })
      .add("twilic encode single-small (direct)", () => {
        encodeDirect(dataset.singleSmall);
      })
      .add("twilic encode single-small (raw json)", () => {
        encodeTransportJson(twilicTransportSingle);
      })
      .add("twilic encode single-small (compact)", () => {
        encodeCompact(dataset.singleSmall);
      })
      .add("twilic encode single-small (compact raw)", () => {
        encodeCompactJson(twilicCompactSingle);
      })
      .add("twilic decode single-small", () => {
        decode(twilicEncodedSingle);
      })
      .add("twilic decode single-small (direct)", () => {
        decodeDirect(twilicEncodedSingleRaw);
      })
      .add("twilic decode single-small (raw json)", () => {
        decodeToTransportJson(twilicEncodedSingleRaw);
      })
      .add("twilic decode single-small (compact raw)", () => {
        decodeToCompactJson(twilicEncodedSingleRaw);
      })
      .add("msgpack encode single-small", () => {
        encodeMsgpack(singleSmallJson);
      })
      .add("msgpack decode single-small", () => {
        decodeMsgpack(msgpackEncodedSingle);
      })
      .add("cbor encode single-small", () => {
        encodeCbor(singleSmallJson);
      })
      .add("cbor decode single-small", () => {
        decodeCbor(cborEncodedSingle);
      })
      .add("bson serialize single-small", () => {
        serializeBson(singleSmallJson as Record<string, unknown>);
      })
      .add("bson deserialize single-small", () => {
        deserializeBson(bsonEncodedSingle);
      })
      .add("twilic encode batch-homogeneous-256", () => {
        encodeBatch(dataset.batchHomogeneous);
      })
      .add("twilic encode batch-homogeneous-256 (direct)", () => {
        encodeBatchDirect(dataset.batchHomogeneous);
      })
      .add("twilic encode batch-homogeneous-256 (raw json)", () => {
        encodeBatchTransportJson(twilicTransportBatchHomogeneous);
      })
      .add("twilic encode batch-homogeneous-256 (compact)", () => {
        encodeBatchCompact(dataset.batchHomogeneous);
      })
      .add("twilic encode batch-homogeneous-256 (compact raw)", () => {
        encodeBatchCompactJson(twilicCompactBatchHomogeneous);
      })
      .add("twilic decode 256 singles", () => {
        for (const encoded of twilicEncodedSingles) {
          decode(encoded);
        }
      })
      .add("msgpack encode batch-homogeneous-256", () => {
        encodeMsgpack(batchHomogeneousJson);
      })
      .add("msgpack decode 256 singles", () => {
        for (const encoded of msgpackEncodedSingles) {
          decodeMsgpack(encoded);
        }
      })
      .add("cbor encode batch-homogeneous-256", () => {
        encodeCbor(batchHomogeneousJson);
      })
      .add("cbor decode 256 singles", () => {
        for (const encoded of cborEncodedSingles) {
          decodeCbor(encoded);
        }
      })
      .add("bson serialize batch-homogeneous-256", () => {
        serializeBson({ records: batchHomogeneousJson });
      })
      .add("bson deserialize 256 singles", () => {
        for (const encoded of bsonEncodedSingles) {
          deserializeBson(encoded);
        }
      })
      .add("twilic encode batch-mixed-256", () => {
        encodeBatch(dataset.batchMixed);
      })
      .add("twilic encode batch-mixed-256 (direct)", () => {
        encodeBatchDirect(dataset.batchMixed);
      })
      .add("twilic encode batch-mixed-256 (raw json)", () => {
        encodeBatchTransportJson(twilicTransportBatchMixed);
      })
      .add("twilic encode batch-mixed-256 (compact)", () => {
        encodeBatchCompact(dataset.batchMixed);
      })
      .add("twilic encode batch-mixed-256 (compact raw)", () => {
        encodeBatchCompactJson(twilicCompactBatchMixed);
      })
      .add("msgpack encode batch-mixed-256", () => {
        encodeMsgpack(batchMixedJson);
      })
      .add("cbor encode batch-mixed-256", () => {
        encodeCbor(batchMixedJson);
      })
      .add("bson serialize batch-mixed-256", () => {
        serializeBson({ records: batchMixedJson });
      })
      .add("twilic patch session-hot", () => {
        patchFlip = !patchFlip;
        sessionEncoder.encodePatch(
          patchFlip ? patchSession.nextA : patchSession.nextB,
        );
      })
      .add("twilic patch session-hot (direct)", () => {
        patchFlipDirect = !patchFlipDirect;
        directSessionEncoder.encodePatchDirect(
          patchFlipDirect ? patchSession.nextA : patchSession.nextB,
        );
      })
      .add("twilic patch session-hot (raw json)", () => {
        patchFlipRaw = !patchFlipRaw;
        rawSessionEncoder.encodePatchTransportJson(
          patchFlipRaw
            ? patchSession.nextATransport
            : patchSession.nextBTransport,
        );
      })
      .add("twilic patch session-hot (compact)", () => {
        patchFlipCompact = !patchFlipCompact;
        compactSessionEncoder.encodePatchCompact(
          patchFlipCompact ? patchSession.nextA : patchSession.nextB,
        );
      })
      .add("twilic patch session-hot (compact raw)", () => {
        patchFlipCompactRaw = !patchFlipCompactRaw;
        compactSessionEncoder.encodePatchCompactJson(
          patchFlipCompactRaw
            ? patchSession.nextACompact
            : patchSession.nextBCompact,
        );
      });

    if (includeJsonBaseline) {
      bench
        .add("json stringify batch-homogeneous-256", () => {
          JSON.stringify(batchHomogeneousJson);
        })
        .add("json parse batch-homogeneous-256", () => {
          JSON.parse(jsonBatchHomogeneousText);
        })
        .add("json stringify batch-mixed-256", () => {
          JSON.stringify(batchMixedJson);
        })
        .add("json parse batch-mixed-256", () => {
          JSON.parse(jsonBatchMixedText);
        })
        .add("json stringify single-small", () => {
          JSON.stringify(singleSmallJson);
        })
        .add("json parse single-small", () => {
          JSON.parse(jsonSingleText);
        });
    }
  }

  bench
    .add("twilic encode schema-user-record-256 (bound stream)", () => {
      encodeTwilicBoundStream();
    })
    .add("protobuf encode schema-user-record-256 (stream)", () => {
      encodeProtobufStream(schemaUserRecords);
    })
    .add("avro encode schema-user-record-256 (stream)", () => {
      encodeAvroStream(schemaUserRecords);
    });

  await bench.run();

  const sizeTableHead = [
    "payload",
    "twilic (bytes)",
    "msgpack (bytes)",
    "cbor (bytes)",
    "bson (bytes)",
  ];

  if (includeJsonBaseline) {
    sizeTableHead.push("json (bytes)");
  }

  sizeTableHead.push("vs msgpack", "vs cbor", "vs bson");

  if (includeJsonBaseline) {
    sizeTableHead.push("vs json");
  }

  const sizeTable = new Table({
    head: sizeTableHead,
    style: { head: [], border: [] },
  });

  const sizeRows = [
    {
      payload: "single-small",
      twilic: twilicEncodedSingle.byteLength,
      msgpack: msgpackEncodedSingle.byteLength,
      cbor: cborEncodedSingle.byteLength,
      bson: bsonEncodedSingle.byteLength,
      json: jsonEncodedSingle.byteLength,
    },
    {
      payload: "batch-homogeneous-256",
      twilic: twilicEncodedBatchHomogeneous.byteLength,
      msgpack: msgpackEncodedBatchHomogeneous.byteLength,
      cbor: cborEncodedBatchHomogeneous.byteLength,
      bson: bsonEncodedBatchHomogeneous.byteLength,
      json: jsonEncodedBatchHomogeneous.byteLength,
    },
    {
      payload: "batch-mixed-256",
      twilic: twilicEncodedBatchMixed.byteLength,
      msgpack: msgpackEncodedBatchMixed.byteLength,
      cbor: cborEncodedBatchMixed.byteLength,
      bson: bsonEncodedBatchMixed.byteLength,
      json: jsonEncodedBatchMixed.byteLength,
    },
    {
      payload: "session-patch-hot (first)",
      twilic: twilicEncodedPatchSessionFirst.byteLength,
      msgpack: encodeMsgpack(patchSession.first as Record<string, unknown>)
        .byteLength,
      cbor: encodeCbor(patchSession.first as Record<string, unknown>)
        .byteLength,
      bson: serializeBson(patchSession.first as Record<string, unknown>)
        .byteLength,
      json: toJsonBytes(patchSession.first as Record<string, unknown>)
        .byteLength,
    },
  ];

  const schemaSizeRows: SchemaSizeRowData[] = [
    {
      payload: "schema-user-record-256 (shared schema stream)",
      twilicBound: twilicBoundSchemaStream.reduce(
        (total, chunk) => total + chunk.byteLength,
        0,
      ),
      protobuf: protobufSchemaStream.byteLength,
      avro: avroSchemaStream.byteLength,
    },
  ];

  const schemaSizeTable = new Table({
    head: [
      "payload",
      "twilic bound (bytes)",
      "protobuf (bytes)",
      "avro (bytes)",
      "vs protobuf",
      "vs avro",
    ],
    style: { head: [], border: [] },
  });

  for (const row of schemaSizeRows) {
    schemaSizeTable.push([
      row.payload,
      formatBytes(row.twilicBound),
      formatBytes(row.protobuf),
      formatBytes(row.avro),
      formatReduction(row.twilicBound, row.protobuf),
      formatReduction(row.twilicBound, row.avro),
    ]);
  }

  for (const row of sizeRows) {
    const tableRow = [
      row.payload,
      formatBytes(row.twilic),
      formatBytes(row.msgpack),
      formatBytes(row.cbor),
      formatBytes(row.bson),
    ];

    if (includeJsonBaseline) {
      tableRow.push(formatBytes(row.json));
    }

    tableRow.push(
      formatReduction(row.twilic, row.msgpack),
      formatReduction(row.twilic, row.cbor),
      formatReduction(row.twilic, row.bson),
    );

    if (includeJsonBaseline) {
      tableRow.push(formatReduction(row.twilic, row.json));
    }

    sizeTable.push(tableRow);
  }

  const fastestHz = bench.tasks.reduce((maxHz, task) => {
    const hz = task.result?.hz ?? 0;
    return Math.max(maxHz, hz);
  }, 0);

  const resultTable = new Table({
    head: ["task", "ops/s", "ns/op", "relative", "rme"],
    style: { head: [], border: [] },
  });

  const sortedTasks = [...bench.tasks].sort((a, b) => {
    const aHz = a.result?.hz ?? 0;
    const bHz = b.result?.hz ?? 0;
    return bHz - aHz;
  });

  for (const task of sortedTasks) {
    const hz = task.result?.hz ?? 0;
    const rme = task.result?.rme;
    resultTable.push([
      task.name,
      formatOps(hz),
      formatNsPerOp(hz),
      formatRelativeSpeed(hz, fastestHz),
      typeof rme === "number" ? formatPercent(rme) : "n/a",
    ]);
  }

  console.log("Twilic benchmark");
  console.log(`runtime: ${runtime}`);
  console.log(`backend preference: ${options.backend}`);
  console.log(`mode: ${options.mode}`);
  console.log(
    `baseline view: ${includeJsonBaseline ? "twilic, msgpack, json" : "twilic, msgpack"}`,
  );
  console.log(`time per task: ${options.timeMs} ms`);
  console.log(`warmup per task: ${options.warmupMs} ms`);
  console.log("");
  console.log("encoded size comparison");
  console.log(sizeTable.toString());
  console.log("");
  console.log("schema-shared encoded size comparison");
  console.log(schemaSizeTable.toString());
  console.log("");
  console.log("results");
  console.log(resultTable.toString());

  if (options.markdownOut) {
    const md = buildMarkdownReport({
      runtime,
      options,
      includeJsonBaseline,
      sizeRows,
      schemaSizeRows,
      sortedTasks,
      fastestHz,
    });
    fs.appendFileSync(options.markdownOut, `${md}\n\n`);
  }

  if (options.jsonOut) {
    const payload = {
      runtime,
      backend: options.backend,
      mode: options.mode,
      timeMs: options.timeMs,
      warmupMs: options.warmupMs,
      includeJsonBaseline,
      tasks: sortedTasks.map((task) => ({
        name: task.name,
        hz: task.result?.hz ?? 0,
        rme: task.result?.rme ?? null,
        samples: task.result?.samples?.length ?? null,
      })),
      sizes: sizeRows.map((row) => ({
        payload: row.payload,
        twilic: row.twilic,
        msgpack: row.msgpack,
        cbor: row.cbor,
        bson: row.bson,
        json: row.json,
      })),
      schemaSizes: schemaSizeRows,
    };
    fs.writeFileSync(options.jsonOut, `${JSON.stringify(payload, null, 2)}\n`);
  }
}

void run();
