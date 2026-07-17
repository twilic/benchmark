import avro from "avsc";
import protobuf from "protobufjs";

export interface UserRecord {
  id: number;
  role: string;
  age?: number;
  active: boolean;
}

const USER_RECORD_PROTO = `
syntax = "proto3";

message UserRecordV1 {
  uint32 id = 1;
  string role = 2;
  uint32 age = 3;
  bool active = 4;
}
`;

const USER_RECORD_AVRO_SCHEMA = {
  type: "record",
  name: "UserRecordV1",
  fields: [
    { name: "id", type: "int" },
    { name: "role", type: "string" },
    { name: "age", type: ["null", "int"], default: null },
    { name: "active", type: "boolean" },
  ],
};

const protobufType = protobuf
  .parse(USER_RECORD_PROTO)
  .root.lookupType("UserRecordV1");
const avroType = avro.Type.forSchema(
  USER_RECORD_AVRO_SCHEMA as Parameters<typeof avro.Type.forSchema>[0],
);

export function makeSchemaUserRecordBatch(): UserRecord[] {
  const roles = ["viewer", "editor", "admin"] as const;
  return Array.from({ length: 256 }, (_, index) => {
    const id = index + 1;
    const record: UserRecord = {
      id,
      role: roles[id % roles.length],
      active: id % 2 === 0,
    };
    if (id % 3 !== 0) {
      record.age = 18 + (id % 50);
    }
    return record;
  });
}

function concat(chunks: Uint8Array[]): Uint8Array {
  const size = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const output = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

/** Encodes records without an OCF header because the schema is shared out of band. */
export function encodeAvroStream(records: UserRecord[]): Uint8Array {
  return concat(
    records.map((record) =>
      avroType.toBuffer({ ...record, age: record.age ?? null }),
    ),
  );
}

/** Encodes independent messages without transport framing because the schema is shared. */
export function encodeProtobufStream(records: UserRecord[]): Uint8Array {
  return concat(
    records.map((record) =>
      protobufType.encode(protobufType.create(record)).finish(),
    ),
  );
}
