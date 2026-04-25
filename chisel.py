#!/usr/bin/env python3
#
# chisel — Avro schema → header-only C++17 encode/decode library generator.
# Requires the argparse and json libraries
#

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Union

# ── IR ─────────────────────────────────────────────────────────────────────────

@dataclass
class Primitive:
    name: str  # long | float | boolean | null | string | bytes

@dataclass
class Ref:
    name: str  # reference to a previously defined named type

@dataclass
class EnumType:
    name: str
    symbols: list[str]

@dataclass
class ArrayType:
    items: 'AvroType'

@dataclass
class FieldDef:
    name: str
    type: 'AvroType'

@dataclass
class RecordType:
    name: str
    fields: list[FieldDef]

AvroType = Union[Primitive, Ref, EnumType, ArrayType, RecordType]

@dataclass
class Schema:
    root: RecordType
    named_types: dict[str, Union[RecordType, EnumType]]  # insertion order = parse order

# ── Parser ─────────────────────────────────────────────────────────────────────

_PRIMITIVES = frozenset({'long', 'float', 'boolean', 'null', 'string', 'bytes'})


class SchemaParser:
    def __init__(self) -> None:
        self._named: dict[str, Union[RecordType, EnumType]] = {}

    def parse(self, obj: dict) -> Schema:
        root = self._parse_type(obj)
        if not isinstance(root, RecordType):
            raise ValueError('top-level schema must be a record')
        return Schema(root=root, named_types=self._named)

    def _parse_type(self, obj) -> AvroType:
        if isinstance(obj, str):
            if obj in _PRIMITIVES:
                return Primitive(obj)
            if obj in self._named:
                return Ref(obj)
            raise ValueError(f'unknown type reference: {obj!r}')

        if isinstance(obj, dict):
            kind = obj['type']
            if kind == 'record':
                return self._parse_record(obj)
            if kind == 'enum':
                return self._parse_enum(obj)
            if kind == 'array':
                return ArrayType(items=self._parse_type(obj['items']))
            if kind in _PRIMITIVES:
                return Primitive(kind)
            raise ValueError(f'unsupported schema type: {kind!r}')

        raise ValueError(f'cannot parse schema node: {obj!r}')

    def _parse_record(self, obj: dict) -> RecordType:
        name = obj['name']
        rec = RecordType(name=name, fields=[])
        self._named[name] = rec  # register before fields so nested refs resolve
        rec.fields = [
            FieldDef(name=f['name'], type=self._parse_type(f['type']))
            for f in obj.get('fields', [])
        ]
        return rec

    def _parse_enum(self, obj: dict) -> EnumType:
        name = obj['name']
        e = EnumType(name=name, symbols=list(obj['symbols']))
        self._named[name] = e
        return e

# ── Dependency sort ─────────────────────────────────────────────────────────────

def _type_deps(t: AvroType) -> list[str]:
    if isinstance(t, (Primitive, EnumType)):
        return []
    if isinstance(t, Ref):
        return [t.name]
    if isinstance(t, ArrayType):
        return _type_deps(t.items)
    if isinstance(t, RecordType):
        out: list[str] = []
        for f in t.fields:
            out.extend(_type_deps(f.type))
        return out
    return []


def _topo_sort(named: dict) -> list[str]:
    visited: set[str] = set()
    order: list[str] = []

    def visit(name: str) -> None:
        if name in visited:
            return
        visited.add(name)
        for dep in _type_deps(named[name]):
            if dep in named:
                visit(dep)
        order.append(name)

    for name in named:
        visit(name)
    return order

# ── Code generator ──────────────────────────────────────────────────────────────

_CPP_PRIM = {
    'long':    'int64_t',
    'float':   'float',
    'boolean': 'bool',
    'null':    'std::monostate',
    'string':  'std::string_view',
    'bytes':   'chisel::span<const uint8_t>',
}

_DETAIL_PRIMITIVES = '''\
inline int64_t decode_long(chisel::span<const uint8_t> buf, size_t& pos) {
    uint64_t n = 0;
    int shift = 0;
    while (true) {
        assert(pos < buf.size());
        uint8_t b = buf[pos++];
        n |= static_cast<uint64_t>(b & 0x7f) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return static_cast<int64_t>((n >> 1) ^ -(n & 1));
}

inline float decode_float(chisel::span<const uint8_t> buf, size_t& pos) {
    assert(pos + 4 <= buf.size());
    float v;
    std::memcpy(&v, buf.data() + pos, 4);
    pos += 4;
    return v;
}

inline bool decode_bool(chisel::span<const uint8_t> buf, size_t& pos) {
    assert(pos < buf.size());
    return buf[pos++] != 0;
}

inline std::string_view decode_string(chisel::span<const uint8_t> buf, size_t& pos) {
    const int64_t len = decode_long(buf, pos);
    assert(len >= 0 && pos + static_cast<size_t>(len) <= buf.size());
    std::string_view sv{reinterpret_cast<const char*>(buf.data() + pos), static_cast<size_t>(len)};
    pos += static_cast<size_t>(len);
    return sv;
}

inline chisel::span<const uint8_t> decode_bytes(chisel::span<const uint8_t> buf, size_t& pos) {
    const int64_t len = decode_long(buf, pos);
    assert(len >= 0 && pos + static_cast<size_t>(len) <= buf.size());
    chisel::span<const uint8_t> s{buf.data() + pos, static_cast<size_t>(len)};
    pos += static_cast<size_t>(len);
    return s;
}

inline void encode_long(int64_t val, chisel::span<uint8_t> buf, size_t& pos) {
    uint64_t n = (static_cast<uint64_t>(val) << 1) ^ static_cast<uint64_t>(val >> 63);
    while (n & ~uint64_t{0x7f}) {
        assert(pos < buf.size());
        buf[pos++] = static_cast<uint8_t>((n & 0x7f) | 0x80);
        n >>= 7;
    }
    assert(pos < buf.size());
    buf[pos++] = static_cast<uint8_t>(n);
}

inline void encode_float(float val, chisel::span<uint8_t> buf, size_t& pos) {
    assert(pos + 4 <= buf.size());
    std::memcpy(buf.data() + pos, &val, 4);
    pos += 4;
}

inline void encode_bool(bool val, chisel::span<uint8_t> buf, size_t& pos) {
    assert(pos < buf.size());
    buf[pos++] = val ? uint8_t{1} : uint8_t{0};
}

inline void encode_string(std::string_view val, chisel::span<uint8_t> buf, size_t& pos) {
    encode_long(static_cast<int64_t>(val.size()), buf, pos);
    assert(pos + val.size() <= buf.size());
    std::memcpy(buf.data() + pos, val.data(), val.size());
    pos += val.size();
}

inline void encode_bytes(chisel::span<const uint8_t> val, chisel::span<uint8_t> buf, size_t& pos) {
    encode_long(static_cast<int64_t>(val.size()), buf, pos);
    assert(pos + val.size() <= buf.size());
    std::memcpy(buf.data() + pos, val.data(), val.size());
    pos += val.size();
}'''

_JSON_HELPER_FUNCS = '''\
constexpr std::string_view J_COL_KEY   = "\\033[1;36m";
constexpr std::string_view J_COL_STR   = "\\033[32m";
constexpr std::string_view J_COL_NUM   = "\\033[33m";
constexpr std::string_view J_COL_BOOL  = "\\033[35m";
constexpr std::string_view J_COL_NULL  = "\\033[2;37m";
constexpr std::string_view J_COL_RESET = "\\033[0m";

inline void json_col(std::ostream& os, std::string_view code, bool on) {
    if (on) os.write(code.data(), static_cast<std::streamsize>(code.size()));
}

inline void json_indent(std::ostream& os, int indent, int depth) {
    os.put('\\n');
    for (int i = 0, n = indent * depth; i < n; ++i) os.put(' ');
}

inline void json_key(std::ostream& os, std::string_view k, bool pretty, bool color) {
    json_col(os, J_COL_KEY, color);
    os.put(0x22);
    os.write(k.data(), static_cast<std::streamsize>(k.size()));
    os.put(0x22);
    os.put(':');
    json_col(os, J_COL_RESET, color);
    if (pretty) os.put(' ');
}

inline void json_string(std::ostream& os, std::string_view s, bool color) {
    json_col(os, J_COL_STR, color);
    os.put(0x22);
    for (unsigned char c : s) {
        if      (c == 0x22) { os.put(0x5c); os.put(0x22); }
        else if (c == 0x5c) { os.put(0x5c); os.put(0x5c); }
        else if (c == 0x08) { os.put(0x5c); os.put('b');  }
        else if (c == 0x0c) { os.put(0x5c); os.put('f');  }
        else if (c == 0x0a) { os.put(0x5c); os.put('n');  }
        else if (c == 0x0d) { os.put(0x5c); os.put('r');  }
        else if (c == 0x09) { os.put(0x5c); os.put('t');  }
        else if (c < 0x20) {
            char buf[7];
            std::snprintf(buf, sizeof(buf), "\\\\u%04x", c);
            os.write(buf, 6);
        } else {
            os.put(static_cast<char>(c));
        }
    }
    os.put(0x22);
    json_col(os, J_COL_RESET, color);
}'''


class CodeGen:
    def __init__(self, schema: Schema, namespace: str) -> None:
        self._ns = namespace
        self._named = schema.named_types
        self._order = _topo_sort(schema.named_types)
        self._root_name = schema.root.name

    # ── type helpers ──────────────────────────────────────────────────────────

    def _cpp_type(self, t: AvroType) -> str:
        if isinstance(t, Primitive):
            return _CPP_PRIM[t.name]
        if isinstance(t, (Ref, RecordType, EnumType)):
            return t.name
        if isinstance(t, ArrayType):
            return f'std::vector<{self._cpp_type(t.items)}>'
        raise AssertionError(t)

    # ── decode expression (returns a C++ expression) ──────────────────────────

    def _decode_expr(self, t: AvroType, buf: str = 'buf', pos: str = 'pos') -> str:
        if isinstance(t, Primitive):
            return {
                'long':    f'detail::decode_long({buf}, {pos})',
                'float':   f'detail::decode_float({buf}, {pos})',
                'boolean': f'detail::decode_bool({buf}, {pos})',
                'null':    'std::monostate{}',
                'string':  f'detail::decode_string({buf}, {pos})',
                'bytes':   f'detail::decode_bytes({buf}, {pos})',
            }[t.name]
        if isinstance(t, (Ref, RecordType, EnumType)):
            ns = '' if t.name == self._root_name else 'detail::'
            return f'{ns}decode_{t.name}({buf}, {pos})'
        if isinstance(t, ArrayType):
            item_t = self._cpp_type(t.items)
            item_e = self._decode_expr(t.items, buf, pos)
            return (
                f'[&]() {{\n'
                f'            std::vector<{item_t}> _v;\n'
                f'            for (int64_t _c = detail::decode_long({buf}, {pos}); _c != 0;\n'
                f'                 _c = detail::decode_long({buf}, {pos})) {{\n'
                f'                if (_c < 0) {{ detail::decode_long({buf}, {pos}); _c = -_c; }}\n'
                f'                _v.reserve(_v.size() + static_cast<size_t>(_c));\n'
                f'                while (_c-- > 0) _v.push_back({item_e});\n'
                f'            }}\n'
                f'            return _v;\n'
                f'        }}()'
            )
        raise AssertionError(t)

    # ── encode statement (returns C++ statement(s)) ───────────────────────────

    def _encode_stmt(self, t: AvroType, val: str,
                     buf: str = 'buf', pos: str = 'pos', ind: int = 4) -> str:
        p = ' ' * ind
        if isinstance(t, Primitive):
            call = {
                'long':    f'detail::encode_long({val}, {buf}, {pos})',
                'float':   f'detail::encode_float({val}, {buf}, {pos})',
                'boolean': f'detail::encode_bool({val}, {buf}, {pos})',
                'null':    f'(void){val}',
                'string':  f'detail::encode_string({val}, {buf}, {pos})',
                'bytes':   f'detail::encode_bytes({val}, {buf}, {pos})',
            }[t.name]
            return f'{p}{call};'
        if isinstance(t, (Ref, RecordType, EnumType)):
            ns = '' if t.name == self._root_name else 'detail::'
            return f'{p}{ns}encode_{t.name}({val}, {buf}, {pos});'
        if isinstance(t, ArrayType):
            item_stmt = self._encode_stmt(t.items, '_item', buf, pos, ind + 8)
            return (
                f'{p}if (!{val}.empty()) {{\n'
                f'{p}    detail::encode_long(static_cast<int64_t>({val}.size()), {buf}, {pos});\n'
                f'{p}    for (const auto& _item : {val}) {{\n'
                f'{item_stmt}\n'
                f'{p}    }}\n'
                f'{p}}}\n'
                f'{p}detail::encode_long(0LL, {buf}, {pos});'
            )
        raise AssertionError(t)

    # ── type definitions ──────────────────────────────────────────────────────

    def _gen_struct(self, r: RecordType) -> str:
        fields = '\n'.join(f'    {self._cpp_type(f.type)} {f.name};' for f in r.fields)
        return f'struct {r.name} {{\n{fields}\n}};'

    def _gen_enum(self, e: EnumType) -> str:
        body = '\n'.join(f'    {sym} = {i},' for i, sym in enumerate(e.symbols))
        return f'enum class {e.name} {{\n{body}\n}};'

    # ── decode functions ──────────────────────────────────────────────────────

    def _gen_decode_record(self, r: RecordType) -> str:
        # C++17 guarantees left-to-right evaluation in braced-init-lists,
        # so pos advances correctly across field initialisers.
        inits = ',\n'.join(
            f'        /* .{f.name} = */ {self._decode_expr(f.type)}'
            for f in r.fields
        )
        return (
            f'inline {r.name} decode_{r.name}(chisel::span<const uint8_t> buf, size_t& pos) {{\n'
            f'    return {r.name}{{\n'
            f'{inits}\n'
            f'    }};\n'
            f'}}'
        )

    def _gen_decode_enum(self, e: EnumType) -> str:
        return (
            f'inline {e.name} decode_{e.name}(chisel::span<const uint8_t> buf, size_t& pos) {{\n'
            f'    return static_cast<{e.name}>(detail::decode_long(buf, pos));\n'
            f'}}'
        )

    # ── encode functions ──────────────────────────────────────────────────────

    def _gen_encode_record(self, r: RecordType) -> str:
        stmts = '\n'.join(self._encode_stmt(f.type, f'val.{f.name}') for f in r.fields)
        return (
            f'inline void encode_{r.name}(const {r.name}& val, chisel::span<uint8_t> buf, size_t& pos) {{\n'
            f'{stmts}\n'
            f'}}'
        )

    def _gen_encode_enum(self, e: EnumType) -> str:
        return (
            f'inline void encode_{e.name}(const {e.name}& val, chisel::span<uint8_t> buf, size_t& pos) {{\n'
            f'    detail::encode_long(static_cast<int64_t>(val), buf, pos);\n'
            f'}}'
        )

    # ── json_print helpers ────────────────────────────────────────────────────

    def _json_val_lines(self, t: AvroType, val: str,
                        ind: str, dep: str, xi: int = 0) -> list[str]:
        """C++ lines that print `val` (of type t) to os. xi drives indentation."""
        p = '    ' * xi
        if isinstance(t, Primitive):
            n = t.name
            if n in ('long', 'float'):
                return [
                    f"{p}detail::json_col(os, detail::J_COL_NUM, color);",
                    f"{p}os << {val};",
                    f"{p}detail::json_col(os, detail::J_COL_RESET, color);",
                ]
            if n == 'boolean':
                return [
                    f"{p}detail::json_col(os, detail::J_COL_BOOL, color);",
                    f'{p}os.write({val} ? "true" : "false", {val} ? 4 : 5);',
                    f"{p}detail::json_col(os, detail::J_COL_RESET, color);",
                ]
            if n == 'null':
                return [
                    f"{p}detail::json_col(os, detail::J_COL_NULL, color);",
                    f'{p}os.write("null", 4);',
                    f"{p}detail::json_col(os, detail::J_COL_RESET, color);",
                ]
            if n == 'string':
                return [f"{p}detail::json_string(os, {val}, color);"]
            if n == 'bytes':
                iv = f'_bi{xi}'
                return [
                    f"{p}os.put('[');",
                    f"{p}for (size_t {iv} = 0; {iv} < {val}.size(); ++{iv}) {{",
                    f"{p}    if ({iv}) os.put(',');",
                    f"{p}    detail::json_col(os, detail::J_COL_NUM, color);",
                    f"{p}    os << static_cast<int>({val}[{iv}]);",
                    f"{p}    detail::json_col(os, detail::J_COL_RESET, color);",
                    f"{p}}}",
                    f"{p}os.put(']');",
                ]
        if isinstance(t, (Ref, RecordType)):
            return [f"{p}detail::json_print_{t.name}(os, {val}, {ind}, {dep}, color);"]
        if isinstance(t, EnumType):
            return [f"{p}detail::json_print_{t.name}(os, {val}, color);"]
        if isinstance(t, ArrayType):
            iv = f'_ai{xi}'
            item_lines = self._json_val_lines(
                t.items, f'{val}[{iv}]', ind, f'{dep} + 1', xi + 1)
            return [
                f"{p}os.put('[');",
                f"{p}for (size_t {iv} = 0; {iv} < {val}.size(); ++{iv}) {{",
                f"{p}    if ({iv}) os.put(',');",
                f"{p}    if (pretty) detail::json_indent(os, {ind}, {dep} + 1);",
                *item_lines,
                f"{p}}}",
                f"{p}if (pretty && !{val}.empty()) detail::json_indent(os, {ind}, {dep});",
                f"{p}os.put(']');",
            ]
        raise AssertionError(t)

    def _gen_json_print_detail_record(self, r: RecordType) -> str:
        lines = [
            f"inline void json_print_{r.name}("
            f"std::ostream& os, const {r.name}& val, int indent, int depth, bool color) {{",
            "    const bool pretty = indent >= 0;",
            "    os.put('{');",
        ]
        for i, f in enumerate(r.fields):
            is_last = (i == len(r.fields) - 1)
            lines.append("    if (pretty) detail::json_indent(os, indent, depth + 1);")
            lines.append(f'    detail::json_key(os, "{f.name}", pretty, color);')
            lines.extend(self._json_val_lines(f.type, f'val.{f.name}',
                                              'indent', 'depth + 1', xi=1))
            if not is_last:
                lines.append("    os.put(',');")
        lines.append("    if (pretty) detail::json_indent(os, indent, depth);")
        lines.append("    os.put('}');")
        lines.append("}")
        return '\n'.join(lines)

    def _gen_json_print_detail_enum(self, e: EnumType) -> str:
        lines = [
            f"inline void json_print_{e.name}(std::ostream& os, {e.name} val, bool color) {{",
            "    detail::json_col(os, detail::J_COL_STR, color);",
            "    os.put(0x22);",
            "    switch (val) {",
        ]
        for sym in e.symbols:
            lines.append(f'        case {e.name}::{sym}: '
                         f'os.write("{sym}", {len(sym)}); break;')
        lines += [
            "    }",
            "    os.put(0x22);",
            "    detail::json_col(os, detail::J_COL_RESET, color);",
            "}",
        ]
        return '\n'.join(lines)

    def _gen_json_print_public(self, t: Union[RecordType, EnumType]) -> str:
        color_check = (
            '    const bool color =\n'
            '        (os.rdbuf() == std::cout.rdbuf() && isatty(STDOUT_FILENO)) ||\n'
            '        (os.rdbuf() == std::cerr.rdbuf() && isatty(STDERR_FILENO));\n'
        )
        if isinstance(t, RecordType):
            return (
                f'inline void json_print(std::ostream& os, const {t.name}& val,\n'
                f'                       int indent = -1) {{\n'
                f'{color_check}'
                f'    detail::json_print_{t.name}(os, val, indent, 0, color);\n'
                f'}}'
            )
        return (
            f'inline void json_print(std::ostream& os, const {t.name}& val) {{\n'
            f'{color_check}'
            f'    detail::json_print_{t.name}(os, val, color);\n'
            f'}}'
        )

    # ── final assembly ────────────────────────────────────────────────────────

    def generate(self) -> str:
        blocks: list[str] = []

        blocks.append(
            '#pragma once\n'
            '#include <cassert>\n'
            '#include <cstdio>\n'
            '#include <cstdint>\n'
            '#include <cstring>\n'
            '#include <iostream>\n'
            '#include <ostream>\n'
            '#include <string_view>\n'
            '#include <type_traits>\n'
            '#include <variant>\n'
            '#include <vector>\n'
            '#include <unistd.h>\n'
            '\n'
            '#ifndef CHISEL_SPAN_DEFINED\n'
            '#define CHISEL_SPAN_DEFINED\n'
            'namespace chisel {\n'
            'template <typename T>\n'
            'struct span {\n'
            '    constexpr span() noexcept : _data(nullptr), _size(0) {}\n'
            '    constexpr span(T* data, std::size_t size) noexcept : _data(data), _size(size) {}\n'
            '    template <typename U,\n'
            '              std::enable_if_t<std::is_same_v<std::remove_const_t<T>, U> && std::is_const_v<T>, int> = 0>\n'
            '    constexpr span(span<U> s) noexcept : _data(s.data()), _size(s.size()) {}\n'
            '    constexpr T*          data()  const noexcept { return _data; }\n'
            '    constexpr std::size_t size()  const noexcept { return _size; }\n'
            '    constexpr bool        empty() const noexcept { return _size == 0; }\n'
            '    constexpr T& operator[](std::size_t i) const noexcept { return _data[i]; }\n'
            '    constexpr T* begin() const noexcept { return _data; }\n'
            '    constexpr T* end()   const noexcept { return _data + _size; }\n'
            'private:\n'
            '    T*          _data;\n'
            '    std::size_t _size;\n'
            '};\n'
            '} // namespace chisel\n'
            '#endif // CHISEL_SPAN_DEFINED\n'
            f'\nnamespace {self._ns} {{'
        )

        fwd = [f'struct {n};'
               for n in self._order
               if isinstance(self._named[n], RecordType)]
        if fwd:
            blocks.append('\n'.join(fwd))

        defs = []
        for n in self._order:
            t = self._named[n]
            if isinstance(t, EnumType):
                defs.append(self._gen_enum(t))
            elif isinstance(t, RecordType):
                defs.append(self._gen_struct(t))
        if defs:
            blocks.append('\n\n'.join(defs))

        detail_parts: list[str] = [_DETAIL_PRIMITIVES, _JSON_HELPER_FUNCS]
        for n in self._order:
            t = self._named[n]
            if isinstance(t, EnumType):
                detail_parts.append(self._gen_json_print_detail_enum(t))
            elif isinstance(t, RecordType):
                detail_parts.append(self._gen_json_print_detail_record(t))
        for n in self._order:
            if n == self._root_name:
                continue
            t = self._named[n]
            if isinstance(t, EnumType):
                detail_parts.append(self._gen_decode_enum(t))
                detail_parts.append(self._gen_encode_enum(t))
            elif isinstance(t, RecordType):
                detail_parts.append(self._gen_decode_record(t))
                detail_parts.append(self._gen_encode_record(t))
        blocks.append('namespace detail {\n\n' + '\n\n'.join(detail_parts) + '\n\n} // namespace detail')

        root_t = self._named[self._root_name]
        assert isinstance(root_t, RecordType)
        blocks.append('\n\n'.join([
            self._gen_decode_record(root_t),
            self._gen_encode_record(root_t),
            self._gen_json_print_public(root_t),
        ]))

        blocks.append(f'}} // namespace {self._ns}')

        return '\n\n'.join(blocks) + '\n'

# ── Entry point ─────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description='Generate a header-only C++17 Avro encode/decode library from a schema.'
    )
    ap.add_argument('schema', type=Path, help='Avro schema JSON file')
    ap.add_argument('-o', '--output', type=Path,
                    help='Output .hpp file (default: <schema-stem>.hpp)')
    ap.add_argument('-n', '--namespace',
                    help='C++ namespace (default: schema file stem)')
    args = ap.parse_args()

    stem = args.schema.stem
    output: Path = args.output or args.schema.with_suffix('.hpp')
    namespace: str = args.namespace or stem

    try:
        raw = json.loads(args.schema.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        sys.exit(f'chisel: {exc}')

    try:
        schema = SchemaParser().parse(raw)
    except (KeyError, ValueError) as exc:
        sys.exit(f'chisel: schema error: {exc}')

    code = CodeGen(schema, namespace).generate()

    try:
        output.write_text(code)
        print(f'chisel: wrote {output}')
    except OSError as exc:
        sys.exit(f'chisel: {exc}')


if __name__ == '__main__':
    main()
