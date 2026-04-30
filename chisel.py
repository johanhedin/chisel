#!/usr/bin/env python3

# chisel - Avro schema → header-only C++17 encode/decode library generator
#
# Copyright (C) 2026 Johan Hedin
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

"""Avro schema → header-only C++17 encode/decode library generator."""

# pylint: disable=too-many-lines

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Union

# ── IR ─────────────────────────────────────────────────────────────────────────

@dataclass
class Primitive:  # pylint: disable=too-few-public-methods
    """An Avro primitive type."""
    name: str  # int | long | float | double | boolean | null | string | bytes

@dataclass
class Ref:  # pylint: disable=too-few-public-methods
    """A reference to a previously defined named type."""
    name: str

@dataclass
class EnumType:  # pylint: disable=too-few-public-methods
    """An Avro enum type."""
    name: str
    symbols: list[str]

@dataclass
class ArrayType:  # pylint: disable=too-few-public-methods
    """An Avro array type."""
    items: 'AvroType'

@dataclass
class OptionalType:  # pylint: disable=too-few-public-methods
    """An Avro [null, T] or [T, null] union, mapped to std::optional<T>."""
    item: 'AvroType'
    null_first: bool  # True when the schema is ["null", T]

@dataclass
class FieldDef:  # pylint: disable=too-few-public-methods
    """A field within a record."""
    name: str
    type: 'AvroType'

@dataclass
class RecordType:  # pylint: disable=too-few-public-methods
    """An Avro record type."""
    name: str
    fields: list[FieldDef]

AvroType = Union[Primitive, Ref, EnumType, ArrayType, OptionalType, RecordType]

@dataclass
class Schema:  # pylint: disable=too-few-public-methods
    """Top-level schema: the root record and all named types in parse order."""
    root: RecordType
    named_types: dict[str, Union[RecordType, EnumType]]

# ── Parser ─────────────────────────────────────────────────────────────────────

_PRIMITIVES = frozenset({'int', 'long', 'float', 'double', 'boolean', 'null', 'string', 'bytes'})


class SchemaParser:  # pylint: disable=too-few-public-methods
    """Parse an Avro schema JSON object into a Schema IR."""

    def __init__(self) -> None:
        self._named: dict[str, Union[RecordType, EnumType]] = {}

    def parse(self, obj: dict) -> Schema:
        """Parse the top-level schema object and return the IR."""
        root = self._parse_type(obj)
        if not isinstance(root, RecordType):
            raise ValueError('top-level schema must be a record')
        named = {k: v for k, v in self._named.items() if v.name == k}
        return Schema(root=root, named_types=named)

    def _parse_type(self, obj) -> AvroType:  # pylint: disable=too-many-return-statements
        if isinstance(obj, str):
            if obj in _PRIMITIVES:
                return Primitive(obj)
            if obj in self._named:
                return Ref(self._named[obj].name)  # resolve alias to canonical name
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

        if isinstance(obj, list):
            if len(obj) == 2 and sum(1 for x in obj if x == 'null') == 1:
                null_first = obj[0] == 'null'
                item_obj = obj[1] if null_first else obj[0]
                return OptionalType(item=self._parse_type(item_obj), null_first=null_first)
            raise ValueError(f'unsupported union shape (only [null, T] supported): {obj!r}')

        raise ValueError(f'cannot parse schema node: {obj!r}')

    def _parse_record(self, obj: dict) -> RecordType:
        name = obj['name']
        rec = RecordType(name=name, fields=[])
        self._named[name] = rec  # register before fields so nested refs resolve
        for alias in obj.get('aliases', []):
            self._named[alias] = rec
        rec.fields = [
            FieldDef(name=f['name'], type=self._parse_type(f['type']))
            for f in obj.get('fields', [])
        ]
        return rec

    def _parse_enum(self, obj: dict) -> EnumType:
        name = obj['name']
        e = EnumType(name=name, symbols=list(obj['symbols']))
        self._named[name] = e
        for alias in obj.get('aliases', []):
            self._named[alias] = e
        return e

# ── Dependency sort ─────────────────────────────────────────────────────────────

def _type_deps(t: AvroType) -> list[str]:
    """Named-type names that field type t directly depends on."""
    if isinstance(t, Primitive):
        return []
    if isinstance(t, EnumType):
        return [t.name]
    if isinstance(t, (Ref, RecordType)):
        return [t.name]
    if isinstance(t, ArrayType):
        return _type_deps(t.items)
    if isinstance(t, OptionalType):
        return _type_deps(t.item)
    return []


def _topo_sort(named: dict) -> list[str]:
    visited: set[str] = set()
    order: list[str] = []

    def visit(name: str) -> None:
        if name in visited:
            return
        visited.add(name)
        t = named[name]
        if isinstance(t, RecordType):
            for f in t.fields:
                for dep in _type_deps(f.type):
                    if dep in named:
                        visit(dep)
        order.append(name)

    for name in named:
        visit(name)
    return order

# ── Code generator ──────────────────────────────────────────────────────────────

_CPP_PRIM = {
    'int':     'int32_t',
    'long':    'int64_t',
    'float':   'float',
    'double':  'double',
    'boolean': 'bool',
    'null':    'std::monostate',
    'string':  'std::string_view',
    'bytes':   'chisel::span<const uint8_t>',
}

_DETAIL_PRIMITIVES = '''\
[[gnu::always_inline]] inline int64_t decode_long(chisel::span<const uint8_t> buf, std::size_t& pos) {
    const std::size_t end = buf.size();
    uint64_t n = 0;
    int shift = 0;
    if (pos + 10 <= end) {
        for (int i = 0; i < 10; ++i) {
            const uint8_t b = buf[pos++];
            n |= static_cast<uint64_t>(b & 0x7f) << shift;
            if (!(b & 0x80))
                return static_cast<int64_t>((n >> 1) ^ -(n & 1));
            shift += 7;
        }
        throw chisel::decode_error("chisel: decode_long: varint too long");
    }
    while (true) {
        if (pos >= end)
            throw chisel::decode_error("chisel: decode_long: buffer underflow");
        if (shift >= 64)
            throw chisel::decode_error("chisel: decode_long: varint too long");
        const uint8_t b = buf[pos++];
        n |= static_cast<uint64_t>(b & 0x7f) << shift;
        if (!(b & 0x80)) break;
        shift += 7;
    }
    return static_cast<int64_t>((n >> 1) ^ -(n & 1));
}

[[gnu::always_inline]] inline int32_t decode_int(chisel::span<const uint8_t> buf, std::size_t& pos) {
    return static_cast<int32_t>(decode_long(buf, pos));
}

[[gnu::always_inline]] inline float decode_float(chisel::span<const uint8_t> buf, std::size_t& pos) {
    if (pos + 4 > buf.size())
        throw chisel::decode_error("chisel: decode_float: buffer underflow");
    uint32_t bits;
    std::memcpy(&bits, buf.data() + pos, 4);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    bits = __builtin_bswap32(bits);
#endif
    float v;
    std::memcpy(&v, &bits, 4);
    pos += 4;
    return v;
}

[[gnu::always_inline]] inline double decode_double(chisel::span<const uint8_t> buf, std::size_t& pos) {
    if (pos + 8 > buf.size())
        throw chisel::decode_error("chisel: decode_double: buffer underflow");
    uint64_t bits;
    std::memcpy(&bits, buf.data() + pos, 8);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    bits = __builtin_bswap64(bits);
#endif
    double v;
    std::memcpy(&v, &bits, 8);
    pos += 8;
    return v;
}

[[gnu::always_inline]] inline bool decode_bool(chisel::span<const uint8_t> buf, std::size_t& pos) {
    if (pos >= buf.size())
        throw chisel::decode_error("chisel: decode_bool: buffer underflow");
    return buf[pos++] != 0;
}

[[gnu::always_inline]] inline std::string_view decode_string(chisel::span<const uint8_t> buf, std::size_t& pos) {
    const int64_t len = decode_long(buf, pos);
    if (len < 0)
        throw chisel::decode_error("chisel: decode_string: negative length");
    if (pos + static_cast<std::size_t>(len) > buf.size())
        throw chisel::decode_error("chisel: decode_string: buffer underflow");
    std::string_view sv{reinterpret_cast<const char*>(buf.data() + pos), static_cast<std::size_t>(len)};
    pos += static_cast<std::size_t>(len);
    return sv;
}

[[gnu::always_inline]] inline chisel::span<const uint8_t> decode_bytes(chisel::span<const uint8_t> buf, std::size_t& pos) {
    const int64_t len = decode_long(buf, pos);
    if (len < 0)
        throw chisel::decode_error("chisel: decode_bytes: negative length");
    if (pos + static_cast<std::size_t>(len) > buf.size())
        throw chisel::decode_error("chisel: decode_bytes: buffer underflow");
    chisel::span<const uint8_t> s{buf.data() + pos, static_cast<std::size_t>(len)};
    pos += static_cast<std::size_t>(len);
    return s;
}

[[gnu::always_inline]] inline void encode_long(int64_t val, chisel::span<uint8_t> buf, std::size_t& pos) {
    uint64_t n = (static_cast<uint64_t>(val) << 1) ^ static_cast<uint64_t>(val >> 63);
    while (n & ~uint64_t{0x7f}) {
        assert(pos < buf.size());
        buf[pos++] = static_cast<uint8_t>((n & 0x7f) | 0x80);
        n >>= 7;
    }
    assert(pos < buf.size());
    buf[pos++] = static_cast<uint8_t>(n);
}

[[gnu::always_inline]] inline void encode_int(int32_t val, chisel::span<uint8_t> buf, std::size_t& pos) {
    encode_long(static_cast<int64_t>(val), buf, pos);
}

[[gnu::always_inline]] inline void encode_float(float val, chisel::span<uint8_t> buf, std::size_t& pos) {
    assert(pos + 4 <= buf.size());
    uint32_t bits;
    std::memcpy(&bits, &val, 4);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    bits = __builtin_bswap32(bits);
#endif
    std::memcpy(buf.data() + pos, &bits, 4);
    pos += 4;
}

[[gnu::always_inline]] inline void encode_double(double val, chisel::span<uint8_t> buf, std::size_t& pos) {
    assert(pos + 8 <= buf.size());
    uint64_t bits;
    std::memcpy(&bits, &val, 8);
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    bits = __builtin_bswap64(bits);
#endif
    std::memcpy(buf.data() + pos, &bits, 8);
    pos += 8;
}

[[gnu::always_inline]] inline void encode_bool(bool val, chisel::span<uint8_t> buf, std::size_t& pos) {
    assert(pos < buf.size());
    buf[pos++] = val ? uint8_t{1} : uint8_t{0};
}

[[gnu::always_inline]] inline void encode_string(std::string_view val, chisel::span<uint8_t> buf, std::size_t& pos) {
    encode_long(static_cast<int64_t>(val.size()), buf, pos);
    assert(pos + val.size() <= buf.size());
    std::memcpy(buf.data() + pos, val.data(), val.size());
    pos += val.size();
}

[[gnu::always_inline]] inline void encode_bytes(chisel::span<const uint8_t> val, chisel::span<uint8_t> buf, std::size_t& pos) {
    encode_long(static_cast<int64_t>(val.size()), buf, pos);
    assert(pos + val.size() <= buf.size());
    std::memcpy(buf.data() + pos, val.data(), val.size());
    pos += val.size();
}

[[gnu::always_inline]] inline void skip_long(chisel::span<const uint8_t> buf, std::size_t& pos) {
    const std::size_t end = buf.size();
    if (pos + 10 <= end) {
        for (int i = 0; i < 10; ++i) {
            if (!(buf[pos++] & 0x80)) return;
        }
        throw chisel::decode_error("chisel: skip_long: varint too long");
    }
    int shift = 0;
    while (true) {
        if (pos >= end)
            throw chisel::decode_error("chisel: skip_long: buffer underflow");
        if (shift >= 64)
            throw chisel::decode_error("chisel: skip_long: varint too long");
        if (!(buf[pos++] & 0x80)) break;
        shift += 7;
    }
}

[[gnu::always_inline]] inline void skip_int(chisel::span<const uint8_t> buf, std::size_t& pos) {
    skip_long(buf, pos);
}

[[gnu::always_inline]] inline void skip_float(chisel::span<const uint8_t> buf, std::size_t& pos) {
    if (pos + 4 > buf.size())
        throw chisel::decode_error("chisel: skip_float: buffer underflow");
    pos += 4;
}

[[gnu::always_inline]] inline void skip_double(chisel::span<const uint8_t> buf, std::size_t& pos) {
    if (pos + 8 > buf.size())
        throw chisel::decode_error("chisel: skip_double: buffer underflow");
    pos += 8;
}

[[gnu::always_inline]] inline void skip_bool(chisel::span<const uint8_t> buf, std::size_t& pos) {
    if (pos >= buf.size())
        throw chisel::decode_error("chisel: skip_bool: buffer underflow");
    ++pos;
}

[[gnu::always_inline]] inline void skip_string(chisel::span<const uint8_t> buf, std::size_t& pos) {
    const int64_t len = decode_long(buf, pos);
    if (len < 0)
        throw chisel::decode_error("chisel: skip_string: negative length");
    if (pos + static_cast<std::size_t>(len) > buf.size())
        throw chisel::decode_error("chisel: skip_string: buffer underflow");
    pos += static_cast<std::size_t>(len);
}

[[gnu::always_inline]] inline void skip_bytes(chisel::span<const uint8_t> buf, std::size_t& pos) {
    const int64_t len = decode_long(buf, pos);
    if (len < 0)
        throw chisel::decode_error("chisel: skip_bytes: negative length");
    if (pos + static_cast<std::size_t>(len) > buf.size())
        throw chisel::decode_error("chisel: skip_bytes: buffer underflow");
    pos += static_cast<std::size_t>(len);
}'''

_JSON_HELPER_FUNCS = '''\
constexpr std::string_view J_COL_KEY   = "\\033[1;36m";
constexpr std::string_view J_COL_STR   = "\\033[32m";
constexpr std::string_view J_COL_NUM   = "\\033[33m";
constexpr std::string_view J_COL_BOOL  = "\\033[35m";
constexpr std::string_view J_COL_NULL  = "\\033[2;37m";
constexpr std::string_view J_COL_RESET = "\\033[0m";

[[gnu::always_inline]] inline void json_col(std::ostream& os, std::string_view code, bool on) {
    if (on) os.write(code.data(), static_cast<std::streamsize>(code.size()));
}

[[gnu::always_inline]] inline void json_indent(std::ostream& os, int indent, int depth) {
    os.put('\\n');
    for (int i = 0, n = indent * depth; i < n; ++i) os.put(' ');
}

[[gnu::always_inline]] inline void json_key(std::ostream& os, std::string_view k, bool pretty, bool color) {
    json_col(os, J_COL_KEY, color);
    os.put(0x22);
    os.write(k.data(), static_cast<std::streamsize>(k.size()));
    os.put(0x22);
    os.put(':');
    json_col(os, J_COL_RESET, color);
    if (pretty) os.put(' ');
}

[[gnu::always_inline]] inline void json_string(std::ostream& os, std::string_view s, bool color) {
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
}

[[gnu::always_inline]] inline void json_bytes(std::ostream& os, chisel::span<const uint8_t> s, bool color) {
    json_col(os, J_COL_STR, color);
    os.put(0x22);
    for (uint8_t c : s) {
        if (c >= 0x20 && c <= 0x7e && c != 0x22 && c != 0x5c) {
            os.put(static_cast<char>(c));
        } else {
            char buf[7];
            std::snprintf(buf, sizeof(buf), "\\\\u%04x", c);
            os.write(buf, 6);
        }
    }
    os.put(0x22);
    json_col(os, J_COL_RESET, color);
}'''


def _indent(code: str, spaces: int) -> str:
    """Add `spaces` leading spaces to every non-empty line of `code`."""
    p = ' ' * spaces
    return '\n'.join(p + line if line else line for line in code.split('\n'))


class CodeGen:  # pylint: disable=too-few-public-methods
    """Generate a C++17 header from a parsed Schema IR."""

    def __init__(self, schema: Schema) -> None:
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
        if isinstance(t, OptionalType):
            return f'std::optional<{self._cpp_type(t.item)}>'
        raise AssertionError(t)

    # ── decode expression (returns a C++ expression) ──────────────────────────

    def _decode_expr(self, t: AvroType, buf: str = 'buf',  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-return-statements
                     pos: str = 'pos', ind: int = 8) -> str:
        if isinstance(t, Primitive):
            return {
                'int':     f'chisel::detail::decode_int({buf}, {pos})',
                'long':    f'chisel::detail::decode_long({buf}, {pos})',
                'float':   f'chisel::detail::decode_float({buf}, {pos})',
                'double':  f'chisel::detail::decode_double({buf}, {pos})',
                'boolean': f'chisel::detail::decode_bool({buf}, {pos})',
                'null':    'std::monostate{}',
                'string':  f'chisel::detail::decode_string({buf}, {pos})',
                'bytes':   f'chisel::detail::decode_bytes({buf}, {pos})',
            }[t.name]
        if isinstance(t, EnumType):
            return f'{self._root_name}::decode_{t.name}({buf}, {pos})'
        if isinstance(t, RecordType):
            return f'{t.name}::decode({buf}, {pos})'
        if isinstance(t, Ref):
            if isinstance(self._named[t.name], EnumType):
                return f'{self._root_name}::decode_{t.name}({buf}, {pos})'
            return f'{t.name}::decode({buf}, {pos})'
        if isinstance(t, ArrayType):
            close = ' ' * ind          # column of the closing }()
            body  = ' ' * (ind + 4)    # lambda body
            cont  = ' ' * (ind + 4 + 5)  # for-loop continuation alignment
            inner = ' ' * (ind + 8)    # for-loop interior
            item_t = self._cpp_type(t.items)
            item_e = self._decode_expr(t.items, buf, pos, ind + 8)
            return (
                f'[&]() {{\n'
                f'{body}std::vector<{item_t}> _v;\n'
                f'{body}for (int64_t _c = chisel::detail::decode_long({buf}, {pos});'
                f' _c != 0;\n'
                f'{cont}_c = chisel::detail::decode_long({buf}, {pos})) {{\n'
                f'{inner}if (_c < 0) {{'
                f' chisel::detail::skip_long({buf}, {pos}); _c = -_c; }}\n'
                f'{inner}_v.reserve(_v.size() + static_cast<std::size_t>(_c));\n'
                f'{inner}while (_c-- > 0) _v.push_back({item_e});\n'
                f'{body}}}\n'
                f'{body}return _v;\n'
                f'{close}}}()'
            )
        if isinstance(t, OptionalType):
            t_arm = 1 if t.null_first else 0
            null_arm = 0 if t.null_first else 1
            inner_t = self._cpp_type(t.item)
            inner_e = self._decode_expr(t.item, buf, pos, ind + 8)
            close = ' ' * ind
            body = ' ' * (ind + 4)
            return (
                f'[&]() -> std::optional<{inner_t}> {{\n'
                f'{body}int64_t _br = chisel::detail::decode_long({buf}, {pos});\n'
                f'{body}if (_br == {t_arm}) {{\n'
                f'{body}    return std::optional<{inner_t}>{{{inner_e}}};\n'
                f'{body}}}\n'
                f'{body}if (_br != {null_arm})'
                f' throw chisel::decode_error("chisel: decode: bad union branch index");\n'
                f'{body}return std::nullopt;\n'
                f'{close}}}()'
            )
        raise AssertionError(t)

    # ── encode statement (returns C++ statement(s)) ───────────────────────────

    def _encode_stmt(self, t: AvroType, val: str,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-return-statements
                     buf: str = 'buf', pos: str = 'pos', ind: int = 4) -> str:
        p = ' ' * ind
        if isinstance(t, Primitive):
            call = {
                'int':     f'chisel::detail::encode_int({val}, {buf}, {pos})',
                'long':    f'chisel::detail::encode_long({val}, {buf}, {pos})',
                'float':   f'chisel::detail::encode_float({val}, {buf}, {pos})',
                'double':  f'chisel::detail::encode_double({val}, {buf}, {pos})',
                'boolean': f'chisel::detail::encode_bool({val}, {buf}, {pos})',
                'null':    f'(void){val}',
                'string':  f'chisel::detail::encode_string({val}, {buf}, {pos})',
                'bytes':   f'chisel::detail::encode_bytes({val}, {buf}, {pos})',
            }[t.name]
            return f'{p}{call};'
        if isinstance(t, EnumType):
            return f'{p}{self._root_name}::encode_{t.name}({val}, {buf}, {pos});'
        if isinstance(t, RecordType):
            return f'{p}{t.name}::encode({val}, {buf}, {pos});'
        if isinstance(t, Ref):
            if isinstance(self._named[t.name], EnumType):
                return f'{p}{self._root_name}::encode_{t.name}({val}, {buf}, {pos});'
            return f'{p}{t.name}::encode({val}, {buf}, {pos});'
        if isinstance(t, ArrayType):
            item_stmt = self._encode_stmt(t.items, '_item', buf, pos, ind + 8)
            return (
                f'{p}if (!{val}.empty()) {{\n'
                f'{p}    chisel::detail::encode_long('
                f'static_cast<int64_t>({val}.size()), {buf}, {pos});\n'
                f'{p}    for (const auto& _item : {val}) {{\n'
                f'{item_stmt}\n'
                f'{p}    }}\n'
                f'{p}}}\n'
                f'{p}chisel::detail::encode_long(0LL, {buf}, {pos});'
            )
        if isinstance(t, OptionalType):
            t_arm = 1 if t.null_first else 0
            null_arm = 0 if t.null_first else 1
            inner_stmt = self._encode_stmt(t.item, f'(*{val})', buf, pos, ind + 4)
            return (
                f'{p}if ({val}.has_value()) {{\n'
                f'{p}    chisel::detail::encode_long({t_arm}LL, {buf}, {pos});\n'
                f'{inner_stmt}\n'
                f'{p}}} else {{\n'
                f'{p}    chisel::detail::encode_long({null_arm}LL, {buf}, {pos});\n'
                f'{p}}}'
            )
        raise AssertionError(t)

    # ── skip statement (returns C++ statement(s) that advance pos) ────────────

    def _skip_stmt(self, t: AvroType, buf: str = 'buf',  # pylint: disable=too-many-return-statements
                   pos: str = 'pos', ind: int = 4) -> str:
        p = ' ' * ind
        if isinstance(t, Primitive):
            if t.name == 'null':
                return ''
            call = {
                'int':     f'chisel::detail::skip_int({buf}, {pos})',
                'long':    f'chisel::detail::skip_long({buf}, {pos})',
                'float':   f'chisel::detail::skip_float({buf}, {pos})',
                'double':  f'chisel::detail::skip_double({buf}, {pos})',
                'boolean': f'chisel::detail::skip_bool({buf}, {pos})',
                'string':  f'chisel::detail::skip_string({buf}, {pos})',
                'bytes':   f'chisel::detail::skip_bytes({buf}, {pos})',
            }[t.name]
            return f'{p}{call};'
        if isinstance(t, EnumType):
            return f'{p}chisel::detail::skip_long({buf}, {pos});'
        if isinstance(t, RecordType):
            return f'{p}{t.name}::skip({buf}, {pos});'
        if isinstance(t, Ref):
            if isinstance(self._named[t.name], EnumType):
                return f'{p}chisel::detail::skip_long({buf}, {pos});'
            return f'{p}{t.name}::skip({buf}, {pos});'
        if isinstance(t, ArrayType):
            item_skip = self._skip_stmt(t.items, buf, pos, ind + 8)
            return (
                f'{p}for (int64_t _c = chisel::detail::decode_long({buf}, {pos}); _c != 0;\n'
                f'{p}     _c = chisel::detail::decode_long({buf}, {pos})) {{\n'
                f'{p}    if (_c < 0) {{'
                f' chisel::detail::skip_long({buf}, {pos}); _c = -_c; }}\n'
                f'{p}    while (_c-- > 0) {{\n'
                f'{item_skip}\n'
                f'{p}    }}\n'
                f'{p}}}'
            )
        if isinstance(t, OptionalType):
            t_arm = 1 if t.null_first else 0
            null_arm = 0 if t.null_first else 1
            inner_skip = self._skip_stmt(t.item, buf, pos, ind + 8)
            return (
                f'{p}{{\n'
                f'{p}    int64_t _br = chisel::detail::decode_long({buf}, {pos});\n'
                f'{p}    if (_br == {t_arm}) {{\n'
                f'{inner_skip}\n'
                f'{p}    }} else if (_br != {null_arm}) {{\n'
                f'{p}        throw chisel::decode_error('
                '"chisel: decode: bad union branch index");\n'
                f'{p}    }}\n'
                f'{p}}}'
            )
        raise AssertionError(t)

    # ── type definitions ──────────────────────────────────────────────────────

    def _gen_enum(self, e: EnumType) -> str:
        body = '\n'.join(f'    {sym} = {i},' for i, sym in enumerate(e.symbols))
        return f'enum class {e.name} {{\n{body}\n}};'

    def _gen_struct_fields(self, r: RecordType) -> str:
        return '\n'.join(f'    {self._cpp_type(f.type)} {f.name};' for f in r.fields)

    # ── codec method emitters (0-indented; use _indent() at call sites) ───────

    def _gen_decode_record(self, r: RecordType) -> str:
        # C++17 guarantees left-to-right evaluation in braced-init-lists,
        # so pos advances correctly across field initialisers. The try/catch
        # rewinds pos on failure so a caller can retry from the same position.
        inits = ',\n'.join(
            f'            /* .{f.name} = */ {self._decode_expr(f.type, ind=12)}'
            for f in r.fields
        )
        return (
            f'static {r.name} decode('
            f'chisel::span<const uint8_t> buf, std::size_t& pos) {{\n'
            f'    const std::size_t _start = pos;\n'
            f'    try {{\n'
            f'        return {r.name}{{\n'
            f'{inits}\n'
            f'        }};\n'
            f'    }} catch (...) {{\n'
            f'        pos = _start;\n'
            f'        throw;\n'
            f'    }}\n'
            f'}}'
        )

    def _gen_encode_record(self, r: RecordType) -> str:
        stmts = '\n'.join(self._encode_stmt(f.type, f'val.{f.name}') for f in r.fields)
        return (
            f'static void encode('
            f'const {r.name}& val, chisel::span<uint8_t> buf, std::size_t& pos) {{\n'
            f'{stmts}\n'
            f'}}'
        )

    def _gen_skip_record(self, r: RecordType) -> str:
        stmts = [self._skip_stmt(f.type, ind=8) for f in r.fields]
        body = '\n'.join(s for s in stmts if s)
        return (
            f'static void skip('
            f'chisel::span<const uint8_t> buf, std::size_t& pos) {{\n'
            f'    const std::size_t _start = pos;\n'
            f'    try {{\n'
            f'{body}\n'
            f'    }} catch (...) {{\n'
            f'        pos = _start;\n'
            f'        throw;\n'
            f'    }}\n'
            f'}}'
        )

    def _gen_array_reader_class(self, arr: ArrayType,  # pylint: disable=too-many-locals
                                cls_name: str) -> str:
        """Generate a nested array-reader class for one array-typed field."""
        item_t = arr.items
        item_is_record = (
            isinstance(item_t, RecordType)
            or (isinstance(item_t, Ref)
                and isinstance(self._named[item_t.name], RecordType))
        )
        # skip one item: used inside _drain/skip (body of while at 16-space indent)
        item_skip_16 = self._skip_stmt(item_t, buf='buf_', pos='pos_', ind=16)

        if item_is_record:
            item_name = item_t.name
            for_each_item = (
                f'            {item_name}::Reader _item{{buf_, pos_}};\n'
                f'            bool _keep = fn(_item);\n'
                f'            _item.skip_remaining();\n'
                f'            if (!_keep) {{\n'
                f'                while (_c-- > 0) {item_name}::skip(buf_, pos_);\n'
                f'                _drain();\n'
                f'                return;\n'
                f'            }}'
            )
        else:
            dec_e = self._decode_expr(item_t, buf='buf_', pos='pos_', ind=12)
            item_skip_20 = self._skip_stmt(item_t, buf='buf_', pos='pos_', ind=20)
            for_each_item = (
                f'            auto _v = {dec_e};\n'
                f'            if (!fn(_v)) {{\n'
                f'                while (_c-- > 0) {{\n'
                f'{item_skip_20}\n'
                f'                }}\n'
                f'                _drain();\n'
                f'                return;\n'
                f'            }}'
            )

        loop_head = (
            '        for (int64_t _c = chisel::detail::decode_long(buf_, pos_); _c != 0;\n'
            '             _c = chisel::detail::decode_long(buf_, pos_)) {\n'
            '            if (_c < 0) {'
            ' chisel::detail::skip_long(buf_, pos_); _c = -_c; }\n'
            '            while (_c-- > 0) {\n'
        )
        loop_tail = (
            '            }\n'
            '        }'
        )

        return (
            f'class {cls_name} {{\n'
            f'public:\n'
            f'    {cls_name}(chisel::span<const uint8_t> buf, std::size_t& pos)\n'
            f'        : buf_(buf), pos_(pos) {{}}\n\n'
            f'    template <typename Fn>\n'
            f'    void for_each(Fn fn) {{\n'
            f'{loop_head}'
            f'{for_each_item}\n'
            f'{loop_tail}\n'
            f'    }}\n\n'
            f'    void skip() {{\n'
            f'{loop_head}'
            f'{item_skip_16}\n'
            f'{loop_tail}\n'
            f'    }}\n\n'
            f'private:\n'
            f'    chisel::span<const uint8_t> buf_;\n'
            f'    std::size_t& pos_;\n\n'
            f'    void _drain() {{\n'
            f'{loop_head}'
            f'{item_skip_16}\n'
            f'{loop_tail}\n'
            f'    }}\n'
            f'}};'
        )

    def _gen_reader_class(self, r: RecordType) -> str:  # pylint: disable=too-many-locals
        """Generate a lazy forward-only Reader nested class for record r."""
        n = len(r.fields)
        pub_parts: list[str] = []

        pub_parts.append(
            'Reader(chisel::span<const uint8_t> buf, std::size_t& pos)\n'
            '    : buf_(buf), pos_(pos), start_(pos), state_(0) {}'
        )
        pub_parts.append(
            f'std::size_t start()    const noexcept {{ return start_; }}\n'
            f'std::size_t position() const noexcept {{ return pos_; }}\n'
            f'bool        done()     const noexcept {{ return state_ == {n}; }}'
        )

        for i, f in enumerate(r.fields):
            if isinstance(f.type, ArrayType):
                cls = ''.join(w.capitalize() for w in f.name.split('_')) + 'Array'
                pub_parts.append(self._gen_array_reader_class(f.type, cls))
                pub_parts.append(
                    f'{cls} read_{f.name}() {{\n'
                    f'    assert(state_ == {i}); ++state_;\n'
                    f'    return {cls}{{buf_, pos_}};\n'
                    f'}}'
                )
                pub_parts.append(
                    f'void skip_{f.name}() {{\n'
                    f'    assert(state_ == {i}); ++state_;\n'
                    f'    {cls}{{buf_, pos_}}.skip();\n'
                    f'}}'
                )
            else:
                cpp_t = self._cpp_type(f.type)
                dec_e = self._decode_expr(f.type, buf='buf_', pos='pos_', ind=4)
                skip_s = self._skip_stmt(f.type, buf='buf_', pos='pos_', ind=4)
                pub_parts.append(
                    f'{cpp_t} read_{f.name}() {{\n'
                    f'    assert(state_ == {i}); ++state_;\n'
                    f'    return {dec_e};\n'
                    f'}}'
                )
                if skip_s:
                    pub_parts.append(
                        f'void skip_{f.name}() {{\n'
                        f'    assert(state_ == {i}); ++state_;\n'
                        f'{skip_s}\n'
                        f'}}'
                    )
                else:
                    pub_parts.append(
                        f'void skip_{f.name}() {{ assert(state_ == {i}); ++state_; }}'
                    )

        if n == 0:
            skip_rem = 'void skip_remaining() {}'
        else:
            cases = '\n'.join(
                (f'        case {i}: skip_{f.name}(); [[fallthrough]];'
                 if i < n - 1
                 else f'        case {i}: skip_{f.name}(); break;')
                for i, f in enumerate(r.fields)
            )
            skip_rem = (
                f'void skip_remaining() {{\n'
                f'    switch (state_) {{\n'
                f'{cases}\n'
                f'    }}\n'
                f'}}'
            )
        pub_parts.append(skip_rem)

        methods = '\n\n'.join(_indent(p, 4) for p in pub_parts)
        return (
            f'class Reader {{\n'
            f'public:\n'
            f'{methods}\n\n'
            f'private:\n'
            f'    chisel::span<const uint8_t> buf_;\n'
            f'    std::size_t& pos_;\n'
            f'    std::size_t  start_;\n'
            f'    int          state_;\n'
            f'}};'
        )

    def _gen_json_print_recursive(self, r: RecordType) -> str:
        lines = [
            f'static void json_print('
            f'std::ostream& os, const {r.name}& val, int indent, int depth, bool color) {{',
            '    const bool pretty = indent >= 0;',
            "    os.put('{');",
        ]
        for i, f in enumerate(r.fields):
            is_last = i == len(r.fields) - 1
            lines.append(
                '    if (pretty) chisel::detail::json_indent(os, indent, depth + 1);')
            lines.append(
                f'    chisel::detail::json_key(os, "{f.name}", pretty, color);')
            lines.extend(self._json_val_lines(f.type, f'val.{f.name}', 'indent', 1, xi=1))
            if not is_last:
                lines.append("    os.put(',');")
        lines.append('    if (pretty) chisel::detail::json_indent(os, indent, depth);')
        lines.append("    os.put('}');")
        lines.append('}')
        return '\n'.join(lines)

    def _gen_json_print_public(self, r: RecordType) -> str:
        return (
            f'static void json_print(std::ostream& os, const {r.name}& val,\n'
            f'                       int indent = -1) {{\n'
            f'    const bool color =\n'
            f'        (os.rdbuf() == std::cout.rdbuf() && isatty(STDOUT_FILENO)) ||\n'
            f'        (os.rdbuf() == std::cerr.rdbuf() && isatty(STDERR_FILENO));\n'
            f'    json_print(os, val, indent, 0, color);\n'
            f'}}'
        )

    def _gen_decode_enum(self, e: EnumType) -> str:
        return (
            f'static {e.name} decode_{e.name}('
            f'chisel::span<const uint8_t> buf, std::size_t& pos) {{\n'
            f'    return static_cast<{e.name}>'
            f'(chisel::detail::decode_long(buf, pos));\n'
            f'}}'
        )

    def _gen_encode_enum(self, e: EnumType) -> str:
        return (
            f'static void encode_{e.name}('
            f'{e.name} val, chisel::span<uint8_t> buf, std::size_t& pos) {{\n'
            f'    chisel::detail::encode_long(static_cast<int64_t>(val), buf, pos);\n'
            f'}}'
        )

    def _gen_json_print_enum(self, e: EnumType) -> str:
        lines = [
            f'static void json_print_{e.name}('
            f'std::ostream& os, {e.name} val, bool color) {{',
            '    chisel::detail::json_col(os, chisel::detail::J_COL_STR, color);',
            '    os.put(0x22);',
            '    switch (val) {',
        ]
        for sym in e.symbols:
            lines.append(
                f'        case {e.name}::{sym}: '
                f'os.write("{sym}", {len(sym)}); break;')
        lines += [
            '    }',
            '    os.put(0x22);',
            '    chisel::detail::json_col(os, chisel::detail::J_COL_RESET, color);',
            '}',
        ]
        return '\n'.join(lines)

    def _gen_nested_record(self, r: RecordType) -> str:
        """Full nested struct with fields and static codec methods (0-indented)."""
        fields = self._gen_struct_fields(r)
        reader_factory = (
            'static Reader reader('
            'chisel::span<const uint8_t> buf, std::size_t& pos) {\n'
            '    return Reader{buf, pos};\n'
            '}'
        )
        methods = '\n\n'.join([
            _indent(self._gen_decode_record(r), 4),
            _indent(self._gen_encode_record(r), 4),
            _indent(self._gen_json_print_recursive(r), 4),
            _indent(self._gen_json_print_public(r), 4),
            _indent(self._gen_skip_record(r), 4),
            _indent(self._gen_reader_class(r), 4),
            _indent(reader_factory, 4),
        ])
        return f'struct {r.name} {{\n{fields}\n\n{methods}\n}};'

    # ── json value lines ──────────────────────────────────────────────────────

    @staticmethod
    def _dep(n: int) -> str:
        """Format an integer depth offset as a C++ depth expression."""
        return 'depth' if n == 0 else f'depth + {n}'

    def _json_val_lines(self, t: AvroType, val: str,  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-return-statements
                        ind: str, dep: int, xi: int = 0) -> list[str]:
        """C++ lines that print `val` (of type t) to os. xi drives indentation."""
        p = '    ' * xi
        if isinstance(t, Primitive):
            n = t.name
            if n in ('int', 'long', 'float', 'double'):
                return [
                    f'{p}chisel::detail::json_col(os, chisel::detail::J_COL_NUM, color);',
                    f'{p}os << {val};',
                    f'{p}chisel::detail::json_col(os, chisel::detail::J_COL_RESET, color);',
                ]
            if n == 'boolean':
                return [
                    f'{p}chisel::detail::json_col(os, chisel::detail::J_COL_BOOL, color);',
                    f'{p}os.write({val} ? "true" : "false", {val} ? 4 : 5);',
                    f'{p}chisel::detail::json_col(os, chisel::detail::J_COL_RESET, color);',
                ]
            if n == 'null':
                return [
                    f'{p}chisel::detail::json_col(os, chisel::detail::J_COL_NULL, color);',
                    f'{p}os.write("null", 4);',
                    f'{p}chisel::detail::json_col(os, chisel::detail::J_COL_RESET, color);',
                ]
            if n == 'string':
                return [f'{p}chisel::detail::json_string(os, {val}, color);']
            if n == 'bytes':
                return [f'{p}chisel::detail::json_bytes(os, {val}, color);']
        if isinstance(t, RecordType):
            return [
                f'{p}{t.name}::json_print(os, {val}, {ind}, {self._dep(dep)}, color);']
        if isinstance(t, EnumType):
            return [
                f'{p}{self._root_name}::json_print_{t.name}(os, {val}, color);']
        if isinstance(t, Ref):
            if isinstance(self._named[t.name], EnumType):
                return [
                    f'{p}{self._root_name}::json_print_{t.name}(os, {val}, color);']
            return [
                f'{p}{t.name}::json_print(os, {val}, {ind}, {self._dep(dep)}, color);']
        if isinstance(t, ArrayType):
            iv = f'_ai{xi}'
            item_lines = self._json_val_lines(
                t.items, f'{val}[{iv}]', ind, dep + 1, xi + 1)
            return [
                f"{p}os.put('[');",
                f'{p}for (std::size_t {iv} = 0; {iv} < {val}.size(); ++{iv}) {{',
                f"{p}    if ({iv}) os.put(',');",
                f'{p}    if (pretty) chisel::detail::json_indent'
                f'(os, {ind}, {self._dep(dep + 1)});',
                *item_lines,
                f'{p}}}',
                f'{p}if (pretty && !{val}.empty())'
                f' chisel::detail::json_indent(os, {ind}, {self._dep(dep)});',
                f"{p}os.put(']');",
            ]
        if isinstance(t, OptionalType):
            inner_lines = self._json_val_lines(t.item, f'(*{val})', ind, dep, xi + 1)
            null_lines = self._json_val_lines(Primitive('null'), val, ind, dep, xi + 1)
            return [
                f'{p}if ({val}.has_value()) {{',
                *inner_lines,
                f'{p}}} else {{',
                *null_lines,
                f'{p}}}',
            ]
        raise AssertionError(t)

    # ── utilities ─────────────────────────────────────────────────────────────

    def _uses_null(self) -> bool:
        """Return True if any field in the schema uses the null primitive."""
        def _check(t: AvroType) -> bool:
            if isinstance(t, Primitive):
                return t.name == 'null'
            if isinstance(t, ArrayType):
                return _check(t.items)
            if isinstance(t, OptionalType):
                return _check(t.item)
            if isinstance(t, RecordType):
                return any(_check(f.type) for f in t.fields)
            return False
        return any(_check(self._named[n]) for n in self._named)

    def _uses_optional(self) -> bool:
        """Return True if any field in the schema uses an optional (union) type."""
        def _check(t: AvroType) -> bool:
            if isinstance(t, OptionalType):
                return True
            if isinstance(t, ArrayType):
                return _check(t.items)
            if isinstance(t, RecordType):
                return any(_check(f.type) for f in t.fields)
            return False
        return any(_check(self._named[n]) for n in self._named)

    # ── final assembly ─────────────────────────────────────────────────────────

    def generate(self) -> str:
        """Emit the complete header as a string."""
        blocks: list[str] = []

        # Includes + chisel::span (guarded)
        optional_include = '#include <optional>\n' if self._uses_optional() else ''
        variant_include = '#include <variant>\n' if self._uses_null() else ''
        blocks.append(
            '#pragma once\n'
            '#include <cassert>\n'
            '#include <cstdio>\n'
            '#include <cstdint>\n'
            '#include <cstring>\n'
            '#include <iostream>\n'
            + optional_include +
            '#include <ostream>\n'
            '#include <stdexcept>\n'
            '#include <string_view>\n'
            '#include <type_traits>\n'
            + variant_include +
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
            '              std::enable_if_t<'
            'std::is_same_v<std::remove_const_t<T>, U> && std::is_const_v<T>, int> = 0>\n'
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
            '#endif // CHISEL_SPAN_DEFINED'
        )

        # chisel::detail — schema-independent primitive and JSON helpers (guarded)
        blocks.append(
            '#ifndef CHISEL_DETAIL_DEFINED\n'
            '#define CHISEL_DETAIL_DEFINED\n'
            'namespace chisel {\n'
            'class decode_error : public std::runtime_error {\n'
            'public:\n'
            '    using std::runtime_error::runtime_error;\n'
            '};\n'
            '} // namespace chisel\n'
            '\n'
            'namespace chisel::detail {\n\n'
            + _DETAIL_PRIMITIVES + '\n\n'
            + _JSON_HELPER_FUNCS + '\n\n'
            '} // namespace chisel::detail\n'
            '#endif // CHISEL_DETAIL_DEFINED'
        )

        # Root struct
        root_parts: list[str] = []

        # Forward declarations of non-root record types
        fwd = [f'    struct {n};'
               for n in self._order
               if isinstance(self._named[n], RecordType) and n != self._root_name]
        if fwd:
            root_parts.append('\n'.join(fwd))

        # Enum class definitions + non-root record definitions in dependency order
        type_defs: list[str] = []
        for n in self._order:
            if n == self._root_name:
                continue
            t = self._named[n]
            if isinstance(t, EnumType):
                type_defs.append(_indent(self._gen_enum(t), 4))
            elif isinstance(t, RecordType):
                type_defs.append(_indent(self._gen_nested_record(t), 4))
        if type_defs:
            root_parts.append('\n\n'.join(type_defs))

        # Root fields
        root_t = self._named[self._root_name]
        assert isinstance(root_t, RecordType)
        root_parts.append(self._gen_struct_fields(root_t))

        # Root codec methods
        root_reader_factory = (
            'static Reader reader('
            'chisel::span<const uint8_t> buf, std::size_t& pos) {\n'
            '    return Reader{buf, pos};\n'
            '}'
        )
        root_parts.append('\n\n'.join([
            _indent(self._gen_decode_record(root_t), 4),
            _indent(self._gen_encode_record(root_t), 4),
            _indent(self._gen_json_print_recursive(root_t), 4),
            _indent(self._gen_json_print_public(root_t), 4),
            _indent(self._gen_skip_record(root_t), 4),
            _indent(self._gen_reader_class(root_t), 4),
            _indent(root_reader_factory, 4),
        ]))

        # Private enum codec helpers (decode_T / encode_T / json_print_T)
        enum_types = [
            self._named[n] for n in self._order
            if isinstance(self._named[n], EnumType)
        ]
        if enum_types:
            private_parts: list[str] = []
            for e in enum_types:
                assert isinstance(e, EnumType)
                private_parts.append(_indent(self._gen_decode_enum(e), 4))
                private_parts.append(_indent(self._gen_encode_enum(e), 4))
                private_parts.append(_indent(self._gen_json_print_enum(e), 4))
            root_parts.append('private:\n\n' + '\n\n'.join(private_parts))

        root_body = '\n\n'.join(root_parts)
        blocks.append(f'struct {self._root_name} {{\n{root_body}\n}};')

        return '\n\n'.join(blocks) + '\n'

# ── Test-helpers generator ──────────────────────────────────────────────────────

class TestHelpersGen:  # pylint: disable=too-few-public-methods
    """Generate a test-helpers header with chisel::test::Generator specialisations."""

    _GENERATOR_CLASS = '''\
class Generator {
public:
    explicit Generator(uint64_t seed)
        : rng_(static_cast<std::mt19937::result_type>(seed)) {}
    template <typename T> T make();
private:
    static constexpr int MAX_MAKE_DEPTH = 4;
    std::mt19937 rng_;
    std::deque<std::string>          str_arena_;
    std::deque<std::vector<uint8_t>> byte_arena_;
    int make_depth_ = 0;

    int32_t make_int() {
        return std::uniform_int_distribution<int32_t>(
            std::numeric_limits<int32_t>::min(),
            std::numeric_limits<int32_t>::max())(rng_);
    }
    int64_t make_long() {
        return std::uniform_int_distribution<int64_t>(
            -(int64_t{1} << 31), (int64_t{1} << 31) - 1)(rng_);
    }
    float make_float() {
        return std::uniform_real_distribution<float>(-1e6f, 1e6f)(rng_);
    }
    double make_double() {
        return std::uniform_real_distribution<double>(-1e15, 1e15)(rng_);
    }
    bool make_bool() {
        return std::bernoulli_distribution(0.5)(rng_);
    }
    std::string_view make_string() {
        static constexpr char CHARS[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        std::size_t n = std::uniform_int_distribution<std::size_t>(0, 20)(rng_);
        std::string s;
        s.reserve(n);
        std::uniform_int_distribution<std::size_t> pick(0, 61);
        for (std::size_t i = 0; i < n; ++i) s += CHARS[pick(rng_)];
        str_arena_.push_back(std::move(s));
        return str_arena_.back();
    }
    chisel::span<const uint8_t> make_bytes() {
        std::size_t n = std::uniform_int_distribution<std::size_t>(0, 16)(rng_);
        std::vector<uint8_t> v(n);
        std::uniform_int_distribution<unsigned> byte_dist(0, 255);
        for (auto& b : v) b = static_cast<uint8_t>(byte_dist(rng_));
        byte_arena_.push_back(std::move(v));
        return {byte_arena_.back().data(), byte_arena_.back().size()};
    }
    std::size_t make_array_len() {
        return std::uniform_int_distribution<std::size_t>(0, 5)(rng_);
    }
};'''

    def __init__(self, schema: Schema) -> None:
        self._named = schema.named_types
        self._order = _topo_sort(schema.named_types)
        self._root = schema.root.name

    def _qual(self, name: str) -> str:
        """Fully-qualified C++ name as seen from outside the root struct."""
        return name if name == self._root else f'{self._root}::{name}'

    def _make_expr(self, t: AvroType) -> str:
        """C++ expression that produces a random value of the given type."""
        if isinstance(t, Primitive):
            return {
                'int':     'make_int()',
                'long':    'make_long()',
                'float':   'make_float()',
                'double':  'make_double()',
                'boolean': 'make_bool()',
                'null':    'std::monostate{}',
                'string':  'make_string()',
                'bytes':   'make_bytes()',
            }[t.name]
        if isinstance(t, (EnumType, RecordType)):
            return f'make<{self._qual(t.name)}>()'
        if isinstance(t, Ref):
            return f'make<{self._qual(t.name)}>()'
        raise AssertionError(t)

    def _cpp_type(self, t: AvroType) -> str:
        if isinstance(t, Primitive):
            return _CPP_PRIM[t.name]
        if isinstance(t, (Ref, RecordType, EnumType)):
            return self._qual(t.name)
        if isinstance(t, ArrayType):
            return f'std::vector<{self._cpp_type(t.items)}>'
        if isinstance(t, OptionalType):
            return f'std::optional<{self._cpp_type(t.item)}>'
        raise AssertionError(t)

    def _fill_array_lines(self, target: str, at: ArrayType, depth: int = 0) -> list[str]:
        """Lines (unindented) that fill `target` (a std::vector) with random items."""
        n_var = f'_n{depth}'
        idx   = f'_i{depth}'
        lines = [
            f'std::size_t {n_var} = make_array_len();',
            f'{target}.reserve({n_var});',
            f'for (std::size_t {idx} = 0; {idx} < {n_var}; ++{idx}) {{',
        ]
        if isinstance(at.items, ArrayType):
            tmp = f'_v{depth}'
            lines.append(f'    {self._cpp_type(at.items)} {tmp};')
            lines += [f'    {l}' for l in self._fill_array_lines(tmp, at.items, depth + 1)]
            lines.append(f'    {target}.push_back(std::move({tmp}));')
        else:
            lines.append(f'    {target}.push_back({self._make_expr(at.items)});')
        lines.append('}')
        return lines

    def _gen_make_record(self, r: RecordType) -> str:
        qual = self._qual(r.name)
        lines = [
            'template <>',
            f'inline {qual} Generator::make<{qual}>() {{',
            f'    {qual} _r{{}};',
            '    ++make_depth_;',
        ]
        for f in r.fields:
            if isinstance(f.type, ArrayType):
                lines.append('    {')
                lines += [f'        {l}' for l in self._fill_array_lines(f'_r.{f.name}', f.type)]
                lines.append('    }')
            elif isinstance(f.type, OptionalType):
                inner = f.type.item
                if isinstance(inner, ArrayType):
                    lines.append('    if (make_depth_ < MAX_MAKE_DEPTH && make_bool()) {')
                    lines.append(f'        _r.{f.name}.emplace();')
                    fill = self._fill_array_lines(f'(*_r.{f.name})', inner)
                    lines += [f'        {l}' for l in fill]
                    lines.append('    }')
                else:
                    item_expr = self._make_expr(inner)
                    cond = 'make_depth_ < MAX_MAKE_DEPTH && make_bool()'
                    lines.append(f'    if ({cond}) _r.{f.name} = {item_expr};')
            else:
                lines.append(f'    _r.{f.name} = {self._make_expr(f.type)};')
        lines += ['    --make_depth_;', '    return _r;', '}']
        return '\n'.join(lines)

    def _gen_make_enum(self, e: EnumType) -> str:
        qual = self._qual(e.name)
        vals = ', '.join(f'{qual}::{s}' for s in e.symbols)
        n = len(e.symbols)
        return (
            f'template <>\n'
            f'inline {qual} Generator::make<{qual}>() {{\n'
            f'    static const {qual} _vals[] = {{{vals}}};\n'
            f'    return _vals[std::uniform_int_distribution<std::size_t>'
            f'(0, {n - 1})(rng_)];\n'
            f'}}'
        )

    def generate(self, codec_hpp: str) -> str:
        """Emit the complete test-helpers header as a string."""
        blocks: list[str] = []

        blocks.append(
            '#pragma once\n'
            f'#include "{codec_hpp}"\n'
            '#include <deque>\n'
            '#include <limits>\n'
            '#include <random>\n'
            '#include <string>\n'
            '#include <vector>\n'
            '\n'
            '#ifndef CHISEL_TEST_DEFINED\n'
            '#define CHISEL_TEST_DEFINED\n'
            'namespace chisel::test {\n\n'
            + self._GENERATOR_CLASS + '\n\n'
            '} // namespace chisel::test\n'
            '#endif // CHISEL_TEST_DEFINED'
        )

        specs: list[str] = []
        for name in self._order:
            t = self._named[name]
            if isinstance(t, RecordType):
                specs.append(self._gen_make_record(t))
            elif isinstance(t, EnumType):
                specs.append(self._gen_make_enum(t))
        if specs:
            blocks.append(
                'namespace chisel::test {\n\n'
                + '\n\n'.join(specs) + '\n\n'
                '} // namespace chisel::test'
            )

        return '\n\n'.join(blocks) + '\n'

# ── Entry point ─────────────────────────────────────────────────────────────────

def main() -> None:
    """Parse CLI arguments and run the code generator."""
    ap = argparse.ArgumentParser(
        description='Generate a header-only C++17 Avro encode/decode library from a schema.'
    )
    ap.add_argument('schema', type=Path, help='Avro schema JSON file')
    ap.add_argument('-o', '--output', type=Path,
                    help='Output file (default: <schema-stem>.hpp or <schema-stem>_test.hpp)')
    ap.add_argument('--test-helpers', action='store_true',
                    help='Emit test-helpers header with random-record builders')
    args = ap.parse_args()

    if args.test_helpers:
        output: Path = args.output or (
            args.schema.parent / (args.schema.stem + '_test.hpp'))
    else:
        output = args.output or args.schema.with_suffix('.hpp')

    try:
        raw = json.loads(args.schema.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        sys.exit(f'chisel: {exc}')

    try:
        schema = SchemaParser().parse(raw)
    except (KeyError, ValueError) as exc:
        sys.exit(f'chisel: schema error: {exc}')

    if args.test_helpers:
        codec_hpp = args.schema.stem + '.hpp'
        code = TestHelpersGen(schema).generate(codec_hpp)
        label = 'test-helpers'
    else:
        code = CodeGen(schema).generate()
        label = 'codec'

    try:
        output.write_text(code)
        print(f'chisel: wrote {label} header {output}')
    except OSError as exc:
        sys.exit(f'chisel: {exc}')


if __name__ == '__main__':
    main()
