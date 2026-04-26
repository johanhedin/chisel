//
// decode_test.cpp - Test program to test the decoding part of chisel generated codecs
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

#include CHISEL_HEADER

// Type alias so the rest of the file is schema-agnostic.
using Root = CHISEL_ROOT;

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <binary-file> [indent]\n"
                  << "  indent  spaces per level for pretty-print (default 4, -1 = compact)\n";
        return 1;
    }
    const int indent = argc >= 3 ? std::atoi(argv[2]) : 4;

    std::ifstream f(argv[1], std::ios::binary);
    if (!f) {
        std::cerr << "cannot open: " << argv[1] << '\n';
        return 1;
    }
    const std::vector<uint8_t> buf(std::istreambuf_iterator<char>(f),
                                   std::istreambuf_iterator<char>{});
    f.close();

    const chisel::span<const uint8_t> span{buf.data(), buf.size()};
    std::size_t pos   = 0;
    std::size_t count = 0;

    while (pos < span.size()) {
        try {
            auto rec = Root::decode(span, pos);
            if (count) std::cout.put('\n');
            Root::json_print(std::cout, rec, indent);
            ++count;
        } catch (const chisel::decode_error& e) {
            std::cerr << "decode error at offset " << pos << ": " << e.what() << '\n';
            return 1;
        }
    }
    if (count) std::cout.put('\n');
    std::cerr << count << " record(s) decoded from " << argv[1] << '\n';
    return 0;
}
