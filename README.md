# Chisel
[![Pylint](https://github.com/johanhedin/chisel/actions/workflows/pylint.yml/badge.svg)](https://github.com/johanhedin/chisel/actions/workflows/pylint.yml)
[![C++ CI](https://github.com/johanhedin/chisel/actions/workflows/ci.yml/badge.svg)](https://github.com/johanhedin/chisel/actions/workflows/ci.yml)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/chisel-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="img/chisel-light.svg">
  <img src="img/chisel-light.svg" alt="chisel logo">
</picture>

`chisel` is a code generation tool that reads an Avro schema and creates a
header-only C++17 decoding/encoding library for raw Avro data streams.

Written with the help of Claude Code.


## Where does `chisel` fit?
You have an uncompressed raw data stream of Avro records in a buffer in a
C++ program and wants to decode them. The data in the buffer is supposed
to be records back-to-back according to a given schema.

If this matches your use case, `chisel` might be a useful tool for you.

> [!NOTE]
> `chisel` only support generating encoders for schemas where the root element
> is of type `record` and where the data buffer given to the decode function
> does not contain a file header (i.e. no embedded schema).


## Requirements
`chisel.py` requires **Python 3.9 or later**. The test helper `stream_gen.py`
additionally requires [fastavro](https://fastavro.readthedocs.io/en/latest/)
(`pip install fastavro`).

The generated C++ code has no external dependencies. It includes a bundled
implementation of std::span, which is not available in the standard C++
library until C++20.


## How to generate code
You need to have an Avro schema in a JSON file. Look at the `registration.json`
for how a schema might look like. Given the schema, generate the decode/encode
library with:

```console
./chisel.py registration.json
```

`chisel` will write a `.hpp` file with the same stem as your schema file. If
you want to adjust the name of the generated file, use the `-o` option:

```console
./chisel.py -o my_file_name.hpp registration.json
```


## How to use the generated code
Given that a library has been generated from the example schema (`registration.json`)
it can be used like:

```c++
#include <vector>
#include "registration.hpp"

// Buffer with raw Avro data
std::vector<uint8_t> buf;

const chisel::span<const uint8_t> span{buf.data(), buf.size()};
std::size_t pos   = 0;
std::size_t count = 0;

while (pos < span.size()) {
    auto reg = Registration::decode(span, pos);
    if (count) std::cout.put('\n');
    Registration::json_print(std::cout, reg, 4);
    ++count;
}
```


## Performance
`chisel` is designed to be fast: all type dispatch is resolved at code-generation
time, and strings and bytes are returned as zero-copy `std::string_view` /
`chisel::span` views into the caller's buffer with no heap allocation per record.

The table below measures a realistic filter workload — scan one million
`Registration` records (185 MB of raw Avro binary) and count matches where any
`readings[].sensor_type` starts with `A` — across four implementations on the
same input:

| Implementation | ns / record | Mrec / s | MB / s | vs Avro C |
|:---|---:|---:|---:|:---:|
| **`chisel` — lazy reader** (`Root::reader`) | **59.4** | **16.8** | **3,255** | **22.7×** |
| `chisel` — eager decode (`Root::decode`) | 93.0 | 10.8 | 2,078 | 14.5× |
| Apache Avro C++ (`avro::decode`) | 799 | 1.25 | 242 | 1.7× |
| Apache Avro C (`avro_value_read`) | 1,351 | 0.74 | 143 | — |

> [!NOTE]
> The gap is structural: `chisel` bakes all type dispatch into the generated code
> and returns strings as zero-copy `std::string_view` views, while Avro C++
> codegen decodes into owning `std::string` fields and wraps each record in a
> decoder object. Avro C interprets the schema at runtime, dispatches through a
> per-field vtable, and `malloc`s every string field.

**Lazy vs eager (1.6×)** — `chisel`'s generated `Reader` skips the entire
`extra_readings` array and all but one field per item, leaving only the bytes
the filter actually needs. The eager `decode` path materialises everything first.

**Chisel eager vs Avro C++ (8.6×)** — both use codegen from the same schema
and do the same decode work. The gap is zero-copy strings (no heap allocation
per field) and the compiler seeing the full decode tree as straight-line
inlinable code rather than library calls through a decoder object.

**Avro C++ vs Avro C (1.7×)** — the C++ codegen path avoids the per-field
vtable dispatch and generic `avro_value_t` overhead of the Avro C value API,
but still pays for owning strings.

Benchmarks live in `bench/` and can be reproduced with `make -C bench run`.

