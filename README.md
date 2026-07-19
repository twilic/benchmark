# Twilic Benchmark

TypeScript benchmark harness for `@twilic/core` (`twilic-js` local package).

## Setup

```bash
pnpm install
```

`@twilic/core` is consumed from `../twilic-js` via a local file dependency. If the native/WASM artifacts are stale, rebuild `twilic-js` first:

```bash
pnpm --dir ../twilic-js build
```

## Run benchmark

```bash
pnpm bench
pnpm bench:msgpack
pnpm bench:max
```

Optional flags:

- `--backend napi|wasm` (default: `napi`)
- `--time-ms <number>` (default: `1000`)
- `--warmup-ms <number>` (default: `250`)
- `--mode full|max` (default: `full`)
- `--twilic-vs-msgpack-only` (hide JSON rows and JSON benchmark tasks)

Examples:

```bash
pnpm bench -- --backend napi
pnpm bench -- --twilic-vs-msgpack-only
pnpm bench -- --mode max --backend napi
pnpm bench -- --backend wasm --time-ms 2000 --warmup-ms 500
```

## What is measured

- Twilic encode/decode for a single record
- Twilic encode/decode for a 256-record batch
- MessagePack encode/decode baselines for single and batched payloads
- Twilic session patch encode (`encodePatch`)
- Raw transport-json fast path (`encodeTransportJson`, `encodeBatchTransportJson`, `decodeToTransportJson`)
- JSON stringify/parse baseline for a comparable payload
- Encoded payload size comparison (`twilic` vs MessagePack vs CBOR vs BSON vs JSON)
- Schema-shared size comparison (`BOUND_STREAM` / `SCHEMA_BATCH` vs Protobuf vs Avro raw streams)
- Pretty CLI tables for size and throughput output (`cli-table3`)

The schema comparison uses the same 256 `UserRecord` values for all formats. Schemas are assumed to be shared out of band, so Avro object-container headers and Protobuf length-prefix framing are deliberately excluded. Twilic measures `encodeBoundStream` and `encodeBatchWithSchema` (not per-message `encodeWithSchema`).

Pinned website numbers live in [`results/pinned-snapshot.json`](results/pinned-snapshot.json). Regenerate with:

```bash
pnpm bench -- --backend napi --time-ms 1500 --warmup-ms 500 --json-out results/pinned-snapshot.json
```

## Max speed tips

- Use `--backend napi` on Node.js
- Prefer Node.js `24+` (project baseline)
- Increase run windows for stability (`--time-ms 3000 --warmup-ms 1000`)
- For hot paths, pre-serialize once with transport-json APIs and use raw encode methods
