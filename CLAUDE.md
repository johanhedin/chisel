# CLAUDE.md
This file provides guidance to Claude Code when working with code in this
repository.


## Project
`chisel` is a code generation tool: given an Avro schema (JSON) for a single
root record type, it generates a header-only C++17 library for decoding and
encoding raw Avro binary data streams containing those records.


## Dependencies & requirements
- **Python ≥ 3.9** — `chisel.py` and `stream_gen.py` use built-in generic type hints
  (`list[str]`, `dict[str, ...]`) that require 3.9+
- **fastavro** — required by `stream_gen.py` (`pip install fastavro`); not needed
  by `chisel.py`
- **g++ with C++17 support** — the generated headers and `decode_test.cpp` require
  `-std=c++17`


## Commands
```bash
# Generate C++ header from a schema
python3 chisel.py <schema.json> [-o output.hpp]

# Generate a raw binary test stream (from test/)
python3 stream_gen.py <schema.json> [-o output.bin] [-n count] [--seed N]

# Makefile targets — run from test/, SCHEMA required for all
make SCHEMA=<schema.json> codec    # generate the .hpp header
make SCHEMA=<schema.json> test     # generate test data, compile and run decode_test
make SCHEMA=<schema.json> clean    # remove all generated artifacts
```


## Code quality
After any change to Python code, run pylint and fix all findings before considering
the work done:

```bash
pylint chisel.py
pylint test/stream_gen.py
```

The target is 10.00/10 with no warnings or errors.


## Testing
An example schema is present in `test/`. To run everything:

```bash
cd test && make SCHEMA=registration.json test
```


## Architecture
### `chisel.py` — code generator
**Parse → IR → Generate** pipeline, all in one file.

- **IR types**: `Primitive`, `Ref`, `EnumType`, `ArrayType`, `FieldDef`, `RecordType`
- **`SchemaParser`**: walks the Avro schema JSON, populates a named-type registry
  as it goes (so forward references like `"items": "ItemRecord"` resolve correctly
  even when the inline definition appeared earlier in the schema)
- **`_topo_sort`**: topological sort of named types so definitions and functions
  are emitted in dependency order
- **`CodeGen`**: generates the header as assembled Python strings — no Jinja2

**Supported Avro types**: `long`, `float`, `boolean`, `null`, `string`, `bytes`,
`enum`, `array`, named `record` references.

### Generated header layout
1. `#pragma once` + includes
2. `chisel::span<T>` — minimal C++17 span (guarded by `CHISEL_SPAN_DEFINED`)
3. `chisel::decode_error` + `chisel::detail` — exception class, Avro binary
   primitive helpers (zig-zag long, float, bool, string, bytes), and JSON
   helpers (colors, indent, key/string/bytes printers). Guarded by
   `CHISEL_DETAIL_DEFINED` so multiple generated headers coexist in one TU.
4. `struct <RootName>` — single struct containing forward declarations of
   non-root records, nested `enum class` and `struct` definitions in
   dependency order (each with their own static codec methods), the root
   record fields, and the root's static `decode` / `encode` / `json_print`
   methods. Enum codec helpers live in a trailing `private:` section.

The root decode uses aggregate braced-init-list initialisation; C++17 guarantees
left-to-right evaluation, so `pos` advances correctly across fields.

### Error handling
Decode helpers in `chisel::detail` throw `chisel::decode_error` (derived from
`std::runtime_error`) when the input stream is corrupt — buffer underflow,
negative length prefix, over-long varint (>10 bytes). Each generated `T::decode`
method wraps its braced-init-list in a try/catch that restores `pos` to its
entry value and rethrows, so `decode` is **atomic** with respect to `pos`: on
exception, `pos` points to the start of the record that failed to decode, and
the caller may retry or skip past it.

Encode helpers still use `assert` for output buffer overflow — that's a
caller bug (insufficient buffer supplied), not an input data problem.

### C++ type mapping
| Avro | C++ |
|------|-----|
| `long` | `int64_t` |
| `float` | `float` |
| `boolean` | `bool` |
| `null` | `std::monostate` |
| `string` | `std::string_view` (zero-copy into raw buffer) |
| `bytes` | `chisel::span<const uint8_t>` (zero-copy) |
| `array<T>` | `std::vector<T>` |
| `enum` | `enum class` |
| `record` | `struct` |

### Wire format
Raw Avro binary, no container format, no embedded schema. Strings and bytes
are decoded as zero-copy views into the original buffer — the buffer must
outlive the decoded record. Encoding writes into a caller-supplied `chisel::span<uint8_t>`
with `pos` advanced by bytes written.

### `test/stream_gen.py` — test data generator
Generates random records matching a schema and writes them as a raw Avro
binary stream using `fastavro.schemaless_writer`. Pre-registers all named
types before generating values (avoids the bug where an empty array skips
inline type registration).

### `test/decode_test.cpp` — generic test harness
Schema-agnostic; takes the schema identity via two compiler defines:
- `CHISEL_HEADER` — path to the generated `.hpp` (e.g. `"registration.hpp"`)
- `CHISEL_ROOT` — the root struct typename (e.g. `Registration`)

### `test/Makefile`
Drives the full generate → compile → run cycle from within `test/`.
Extracts `ROOT` (the Avro `name` field) from the schema JSON via a
`python3 -c` one-liner.
