# Chisel
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="img/chisel-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="img/chisel-light.svg">
  <img src="img/chisel-light.svg" alt="chisel logo">
</picture>

`chisel` is a code generation tool that reads an Avro schema for a record type
and creates a header-only C++17 decoding/encoding library for raw Avro
datastreams containing those records.

Written with the help of Claude Code.


## Where does `chisel` fit?
You have a raw data stream of uncompressed Avro data in a buffer in a C++ program
and want to decode it. The data is supposed to be records back-to-back according
to a given schema.

If this describes your use case, `chisel` might be a useful tool for you.


## How to generate code
You need to have an Avro schema in a JSON file. Look at the `record.json`
for how a schema might look like. Given the schema, generate the decode/encode
library with:

```console
./chisel <name_of_schema_file>
```

`chisel` will write a `.hpp` file with the same stem as your schema. If you want
to adjust the name of the generated namespace, you can use the `-n` option:

```console
./chisel -n my_namespace <name_of_schema_file>
```


## How to use the generated code
Given that a library has been generated from the example schema (`record.json`)
it can be used like:

```c++
#include <vector>
#include "record.hpp"

// Buffer with raw Avro data
std::vector<uint8_t> buf;

const chisel::span<const uint8_t> span{buf.data(), buf.size()};
size_t pos   = 0;
size_t count = 0;

while (pos < buf.size()) {
    auto record = record::decode(span, pos);
    if (count) std::cout.put('\n');
    record::json_print(std::cout, record, 4);
    ++count;
}
```
