//
// encode_test.cpp - Test program to test the encoding part of chisel generated codecs
//
// Copyright (C) 2026 Johan Hedin
//
// This program is free software; you can redistribute it and/or modify it under
// the terms of the GNU General Public License as published by the Free Software
// Foundation; either version 2 of the License, or (at your option) any later
// version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>

#include CHISEL_TEST_HEADER

// Type alias so the rest of the file is schema-agnostic.
using Root = CHISEL_ROOT;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <output-file> [count] [seed]\n"
                  << "  count  number of records to encode (default 20)\n"
                  << "  seed   random seed (default 42)\n";
        return 1;
    }
    const int      count = argc >= 3 ? std::atoi(argv[2]) : 20;
    const uint64_t seed  = argc >= 4 ? static_cast<uint64_t>(std::atoll(argv[3])) : 42;

    std::ofstream out(argv[1], std::ios::binary);
    if (!out) {
        std::cerr << "cannot open: " << argv[1] << '\n';
        return 1;
    }

    std::vector<uint8_t> buf(1 << 20);  // 1 MiB scratch buffer
    const chisel::span<uint8_t> span{buf.data(), buf.size()};
    chisel::test::Generator gen(seed);

    for (int i = 0; i < count; ++i) {
        Root rec = gen.make<Root>();
        std::size_t pos = 0;
        Root::encode(rec, span, pos);
        out.write(reinterpret_cast<const char*>(buf.data()),
                  static_cast<std::streamsize>(pos));
    }
    std::cerr << count << " record(s) encoded to " << argv[1] << '\n';
    return 0;
}
