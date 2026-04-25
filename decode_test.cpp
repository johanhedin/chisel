#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>

#include CHISEL_HEADER

// Namespace alias so the rest of the file is schema-agnostic.
namespace ns = CHISEL_NS;

// CHISEL_DECODE(Root) pastes decode_ and the root type name at compile time.
#define CHISEL_CAT_(a, b) a##b
#define CHISEL_DECODE(root) CHISEL_CAT_(decode_, root)

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
    size_t pos   = 0;
    size_t count = 0;

    while (pos < buf.size()) {
        auto rec = ns::CHISEL_DECODE(CHISEL_ROOT)(span, pos);
        if (count) std::cout.put('\n');
        ns::json_print(std::cout, rec, indent);
        ++count;
    }
    if (count) std::cout.put('\n');
    std::cerr << count << " record(s) decoded from " << argv[1] << '\n';
    return 0;
}
