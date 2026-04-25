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


## Testing
A example schema is present in the project. So to simply test everything, just run:

```bash
make SCHEMA=record.json test
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
4. Forward declarations for all record structs
5. Type definitions in dependency order (`enum class` with explicit integer values, `struct`)
6. `namespace detail {` — single consolidated block:
   - Avro binary primitive helpers: zig-zag long, float, bool, string, bytes encode/decode
   - JSON helpers: color constants, `json_col`, `json_indent`, `json_key`, `json_string`
   - `json_print_T` functions for all types in dependency order
   - `decode_T` / `encode_T` for all **non-root** named types in dependency order
7. `decode(buf, pos)` — root record decode (public); aggregate-initialises via braced-init-list
   (C++17 guarantees left-to-right evaluation, so `pos` advances correctly across fields)
8. `encode(val, buf, pos)` — root record encode (public); writes into caller-supplied
   `chisel::span<uint8_t>`, advances `pos`
9. `json_print(os, val, indent)` — root record JSON print (public); auto-detects color
   when writing to an unredirected stdout/stderr
10. `} // namespace <ns>`

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
Schema-agnostic; takes the schema identity via two compiler defines:
- `CHISEL_HEADER` — path to the generated `.hpp` (e.g. `"record.hpp"`)
- `CHISEL_NS` — C++ namespace (e.g. `record`)

The Makefile extracts `CHISEL_NS` from the filename stem.
