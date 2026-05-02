#!/usr/bin/env python3

# stream_read.py - reads a raw Avro binary stream and prints JSON in chisel json_print format
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

"""Read a raw Avro binary stream and print JSON in chisel json_print format."""

import argparse
import io
import json
import sys
from pathlib import Path

import fastavro

_J_COL_KEY   = '\033[1;36m'  # bold cyan  — keys
_J_COL_STR   = '\033[32m'    # green      — strings, bytes, enums
_J_COL_NUM   = '\033[33m'    # yellow     — int, long, float, double
_J_COL_BOOL  = '\033[35m'    # magenta    — booleans
_J_COL_NULL  = '\033[2;37m'  # dim white  — null
_J_COL_RESET = '\033[0m'


def _emit_string_val(s: str, color: bool) -> str:
    """Escape a string with chisel's json_string byte-level rules."""
    parts = [_J_COL_STR if color else '', '"']
    for b in s.encode('utf-8'):
        if b == 0x22:
            parts.append('\\"')
        elif b == 0x5c:
            parts.append('\\\\')
        elif b == 0x08:
            parts.append('\\b')
        elif b == 0x0c:
            parts.append('\\f')
        elif b == 0x0a:
            parts.append('\\n')
        elif b == 0x0d:
            parts.append('\\r')
        elif b == 0x09:
            parts.append('\\t')
        elif b < 0x20:
            parts.append(f'\\u{b:04x}')
        else:
            parts.append(chr(b))
    parts.append('"')
    if color:
        parts.append(_J_COL_RESET)
    return ''.join(parts)


def _emit_bytes_val(b: bytes, color: bool) -> str:
    """Render bytes with chisel's json_bytes rules (printable ASCII or \\uXXXX)."""
    parts = [_J_COL_STR if color else '', '"']
    for byte in b:
        if 0x20 <= byte <= 0x7e and byte != 0x22 and byte != 0x5c:
            parts.append(chr(byte))
        else:
            parts.append(f'\\u{byte:04x}')
    parts.append('"')
    if color:
        parts.append(_J_COL_RESET)
    return ''.join(parts)


def _collect_aliases(schema, alias_map: dict) -> None:
    """Walk schema and populate alias_map with alias → canonical name entries."""
    if isinstance(schema, list):
        for branch in schema:
            _collect_aliases(branch, alias_map)
    elif isinstance(schema, dict):
        kind = schema.get('type')
        if kind in ('record', 'enum', 'fixed'):
            name = schema['name']
            for alias in schema.get('aliases', []):
                alias_map[alias] = name
            if kind == 'record':
                for f in schema.get('fields', []):
                    _collect_aliases(f['type'], alias_map)
        elif kind == 'array':
            _collect_aliases(schema['items'], alias_map)


def _resolve_aliases(schema, alias_map: dict):
    """Return a copy of schema with alias type references replaced by canonical names."""
    if isinstance(schema, list):
        return [_resolve_aliases(branch, alias_map) for branch in schema]
    if isinstance(schema, str):
        return alias_map.get(schema, schema)
    if isinstance(schema, dict):
        kind = schema.get('type')
        result = dict(schema)
        if kind == 'record':
            result['fields'] = [
                {**f, 'type': _resolve_aliases(f['type'], alias_map)}
                for f in schema.get('fields', [])
            ]
        elif kind == 'array':
            result['items'] = _resolve_aliases(schema['items'], alias_map)
        return result
    return schema


class Emitter:
    """Emit fastavro-decoded records as JSON matching chisel's json_print output."""

    def __init__(self, schema_raw: dict, indent: int, color: bool = False) -> None:
        self._named: dict = {}
        self._indent = indent
        self._color = color
        self._register(schema_raw)

    def _register(self, schema) -> None:
        """Pre-populate named-type registry so string references resolve."""
        if isinstance(schema, list):
            for branch in schema:
                self._register(branch)
        elif isinstance(schema, dict):
            kind = schema.get('type')
            if kind in ('record', 'enum', 'fixed'):
                self._named[schema['name']] = schema
                for alias in schema.get('aliases', []):
                    self._named[alias] = schema
                if kind == 'record':
                    for f in schema.get('fields', []):
                        self._register(f['type'])
            elif kind == 'array':
                self._register(schema['items'])

    def emit(self, out, value, schema, depth: int = 0) -> None:
        """Write value (conforming to schema) to out."""
        if isinstance(schema, list):
            if value is None:
                self._emit_primitive(out, None, 'null')
            else:
                inner = next(b for b in schema if b != 'null')
                self.emit(out, value, inner, depth)
            return
        if isinstance(schema, str):
            if schema in self._named:
                self.emit(out, value, self._named[schema], depth)
                return
            self._emit_primitive(out, value, schema)
            return
        if isinstance(schema, dict):
            kind = schema['type']
            if kind == 'record':
                self._emit_record(out, value, schema, depth)
            elif kind == 'enum':
                if self._color:
                    out.write(f'{_J_COL_STR}"{value}"{_J_COL_RESET}')
                else:
                    out.write(f'"{value}"')
            elif kind == 'array':
                self._emit_array(out, value, schema['items'], depth)
            elif kind == 'fixed':
                out.write(_emit_bytes_val(value, self._color))
            else:
                self._emit_primitive(out, value, kind)

    def _emit_record(self, out, value: dict, schema: dict, depth: int) -> None:
        pretty = self._indent >= 0
        out.write('{')
        fields = schema['fields']
        for i, f in enumerate(fields):
            if pretty:
                out.write('\n' + ' ' * (self._indent * (depth + 1)))
            if self._color:
                out.write(f'{_J_COL_KEY}"{f["name"]}":{ _J_COL_RESET}')
            else:
                out.write(f'"{f["name"]}":')
            if pretty:
                out.write(' ')
            self.emit(out, value[f['name']], f['type'], depth + 1)
            if i < len(fields) - 1:
                out.write(',')
        if pretty:
            out.write('\n' + ' ' * (self._indent * depth))
        out.write('}')

    def _emit_array(self, out, value: list, items_schema, depth: int) -> None:
        pretty = self._indent >= 0
        out.write('[')
        for i, item in enumerate(value):
            if i:
                out.write(',')
            if pretty:
                out.write('\n' + ' ' * (self._indent * (depth + 1)))
            self.emit(out, item, items_schema, depth + 1)
        if pretty and value:
            out.write('\n' + ' ' * (self._indent * depth))
        out.write(']')

    def _col(self, out, code: str, text: str) -> None:
        """Write text wrapped in an ANSI color code when color is enabled."""
        if self._color:
            out.write(f'{code}{text}{_J_COL_RESET}')
        else:
            out.write(text)

    def _emit_primitive(self, out, value, kind: str) -> None:
        if kind == 'null':
            self._col(out, _J_COL_NULL, 'null')
        elif kind == 'boolean':
            self._col(out, _J_COL_BOOL, 'true' if value else 'false')
        elif kind in ('long', 'int'):
            self._col(out, _J_COL_NUM, str(value))
        elif kind in ('float', 'double'):
            self._col(out, _J_COL_NUM, f'{value:g}')
        elif kind == 'string':
            out.write(_emit_string_val(value, self._color))
        elif kind == 'bytes':
            out.write(_emit_bytes_val(value, self._color))
        else:
            raise ValueError(f'stream_read: unsupported primitive: {kind!r}')


def main() -> None:
    """Parse CLI arguments and print the Avro binary stream as JSON."""
    ap = argparse.ArgumentParser(
        description='Read a raw Avro binary stream and print JSON in chisel json_print format.'
    )
    ap.add_argument('schema', type=Path, help='Avro schema JSON file')
    ap.add_argument('binary', type=Path, help='Raw Avro binary stream file')
    ap.add_argument('indent', type=int, nargs='?', default=4,
                    help='Spaces per indent level (default 4, -1 = compact)')
    args = ap.parse_args()

    try:
        raw = json.loads(args.schema.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        sys.exit(f'stream_read: {exc}')

    alias_map: dict = {}
    _collect_aliases(raw, alias_map)
    normalized = _resolve_aliases(raw, alias_map) if alias_map else raw

    try:
        parsed = fastavro.parse_schema(normalized)
    except Exception as exc:
        sys.exit(f'stream_read: schema error: {exc}')

    try:
        data = args.binary.read_bytes()
    except OSError as exc:
        sys.exit(f'stream_read: {exc}')

    emitter = Emitter(raw, args.indent, sys.stdout.isatty())
    buf = io.BytesIO(data)
    count = 0

    try:
        while buf.tell() < len(data):
            rec = fastavro.schemaless_reader(buf, parsed)
            if count:
                sys.stdout.write('\n')
            emitter.emit(sys.stdout, rec, raw, 0)
            count += 1
    except Exception as exc:
        sys.exit(f'stream_read: read error at byte {buf.tell()}: {exc}')

    if count:
        sys.stdout.write('\n')
    sys.stderr.write(f'{count} record(s) read from {args.binary}\n')


if __name__ == '__main__':
    main()
