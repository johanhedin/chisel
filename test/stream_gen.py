#!/usr/bin/env python3

# stream_gen.py - writes a raw Avro binary stream of random records given a schema
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

"""Write a raw Avro binary stream of random records from an Avro schema."""

import argparse
import io
import json
import random
import string
import sys
from pathlib import Path

# We depend on fastavro
import fastavro


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
        elif kind == 'map':
            _collect_aliases(schema['values'], alias_map)


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
        elif kind == 'map':
            result['values'] = _resolve_aliases(schema['values'], alias_map)
        return result
    return schema


class RandomGen:
    """Generates random Python values that match an Avro schema."""

    def __init__(self, root_schema: dict) -> None:
        self._named: dict = {}
        self._register(root_schema)

    def _register(self, schema) -> None:
        """Pre-populate the named-type registry before generating any values."""
        if isinstance(schema, list):
            for branch in schema:
                self._register(branch)
        elif isinstance(schema, dict):
            kind = schema.get('type')
            if kind == 'record':
                self._named[schema['name']] = schema
                for alias in schema.get('aliases', []):
                    self._named[alias] = schema
                for f in schema.get('fields', []):
                    self._register(f['type'])
            elif kind == 'enum':
                self._named[schema['name']] = schema
                for alias in schema.get('aliases', []):
                    self._named[alias] = schema
            elif kind == 'fixed':
                self._named[schema['name']] = schema
                for alias in schema.get('aliases', []):
                    self._named[alias] = schema
            elif kind == 'array':
                self._register(schema['items'])
            elif kind == 'map':
                self._register(schema['values'])

    def value(self, schema, depth=0) -> object:
        """Return a random Python value conforming to the given schema node."""
        if isinstance(schema, list):
            if depth >= 4 and 'null' in schema:
                return None
            return self.value(random.choice(schema), depth)

        if isinstance(schema, str):
            if schema in self._named:
                return self.value(self._named[schema], depth)
            return self._primitive(schema)

        if isinstance(schema, dict):
            kind = schema['type']
            if kind == 'record':
                self._named[schema['name']] = schema
                return {f['name']: self.value(f['type'], depth + 1) for f in schema['fields']}
            if kind == 'enum':
                self._named[schema['name']] = schema
                return random.choice(schema['symbols'])
            if kind == 'fixed':
                return bytes(random.randint(0, 255) for _ in range(schema['size']))
            if kind == 'array':
                if depth >= 4:
                    return []
                n = random.randint(0, 5)
                return [self.value(schema['items'], depth) for _ in range(n)]
            if kind == 'map':
                if depth >= 4:
                    return {}
                n = random.randint(0, 5)
                return {self._primitive('string'): self.value(schema['values'], depth)
                        for _ in range(n)}
            return self._primitive(kind)

        raise ValueError(f'cannot generate value for: {schema!r}')

    @staticmethod
    def _primitive(name: str) -> object:
        if name == 'null':
            return None
        if name == 'boolean':
            return random.choice([True, False])
        if name == 'int':
            return random.randint(-(2**31), 2**31 - 1)
        if name == 'long':
            return random.randint(-(2**31), 2**31 - 1)
        if name == 'float':
            return random.uniform(-1e6, 1e6)
        if name == 'double':
            return random.uniform(-1e15, 1e15)
        if name == 'string':
            n = random.randint(0, 20)
            return ''.join(random.choices(string.ascii_letters + string.digits, k=n))
        if name == 'bytes':
            n = random.randint(0, 16)
            return bytes(random.randint(0, 255) for _ in range(n))
        raise ValueError(f'unsupported primitive: {name!r}')


def main() -> None:
    """Parse CLI arguments and write the binary stream."""
    ap = argparse.ArgumentParser(
        description='Generate a raw Avro binary stream of random records.'
    )
    ap.add_argument('schema', type=Path, help='Avro schema JSON file')
    ap.add_argument('-o', '--output', type=Path,
                    help='Output binary file (default: <schema-stem>.bin)')
    ap.add_argument('-n', '--count', type=int, default=10,
                    help='Number of records to generate (default: 10)')
    ap.add_argument('--seed', type=int,
                    help='Random seed for reproducible output')
    args = ap.parse_args()

    output: Path = args.output or args.schema.with_suffix('.bin')

    if args.seed is not None:
        random.seed(args.seed)

    try:
        raw = json.loads(args.schema.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        sys.exit(f'stream_gen: {exc}')

    alias_map: dict = {}
    _collect_aliases(raw, alias_map)
    normalized = _resolve_aliases(raw, alias_map) if alias_map else raw

    try:
        parsed = fastavro.parse_schema(normalized)
    except Exception as exc:
        sys.exit(f'stream_gen: schema error: {exc}')

    gen = RandomGen(raw)

    try:
        with open(output, 'wb') as f:
            for _ in range(args.count):
                record = gen.value(raw)
                buf = io.BytesIO()
                fastavro.schemaless_writer(buf, parsed, record)
                f.write(buf.getvalue())
        print(f'stream_gen: wrote {args.count} records to {output}')
    except Exception as exc:
        sys.exit(f'stream_gen: {exc}')


if __name__ == '__main__':
    main()
