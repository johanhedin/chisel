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
python3 chisel.py <schema.json> [-o output.hpp] [-n namespace]

# Generate a raw binary test stream
python3 stream_gen.py <schema.json> [-o output.bin] [-n count] [--seed N]

# Makefile targets (SCHEMA required for all)
make SCHEMA=<schema.json> codec    # generate the .hpp header
make SCHEMA=<schema.json> test     # generate test data, compile and run decode_test
make SCHEMA=<schema.json> clean    # remove all generated artifacts
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
2. `chisel::span<T>` — minimal C++17 span (guarded by `#ifndef CHISEL_SPAN_DEFINED`
   so multiple generated headers coexist)
3. `namespace <ns> {`
4. `namespace detail` — Avro binary helpers (zig-zag long, float, bool, string,
   bytes encode/decode)
5. Forward declarations for all record structs
6. Type definitions in dependency order (`enum class` with explicit integer values, `struct`)
7. `decode_T` free functions — aggregate-initialise the struct via braced-init-list
   (C++17 guarantees left-to-right evaluation, so `pos` advances correctly across fields)
8. `encode_T` free functions — write into caller-supplied `chisel::span<uint8_t>`,
   advance `pos`
9. `namespace detail` (reopened) — JSON print helpers (color constants, `json_col`,
   `json_indent`, `json_key`, `json_string`) + per-type `json_print_T` detail functions
10. Public `json_print(std::ostream&, const T&, int indent = -1)` overloads — auto-detect
   color when writing to an unredirected stdout/stderr

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

### `stream_gen.py` — test data generator
Generates random records matching a schema and writes them as a raw Avro
binary stream using `fastavro.schemaless_writer`. Pre-registers all named
types before generating values (avoids the bug where an empty array skips
inline type registration).

### `decode_test.cpp` — generic test harness
Schema-agnostic; takes the schema identity via three compiler defines:
- `CHISEL_HEADER` — path to the generated `.hpp` (e.g. `"schema_example.hpp"`)
- `CHISEL_NS` — C++ namespace (e.g. `schema_example`)
- `CHISEL_ROOT` — root record type name (e.g. `Record`)

Uses `##` token-pasting (`CHISEL_DECODE(CHISEL_ROOT)` → `decode_Record`) to
call the correct decode function without knowing the type name at source-writing
time. The Makefile extracts `CHISEL_NS` from the filename stem and `CHISEL_ROOT`
from the schema JSON's `"name"` field.
