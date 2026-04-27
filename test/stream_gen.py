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


class RandomGen:  # pylint: disable=too-few-public-methods
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
                for f in schema.get('fields', []):
                    self._register(f['type'])
            elif kind == 'enum':
                self._named[schema['name']] = schema
            elif kind == 'array':
                self._register(schema['items'])

    def value(self, schema) -> object:  # pylint: disable=too-many-return-statements
        """Return a random Python value conforming to the given schema node."""
        if isinstance(schema, list):
            return self.value(random.choice(schema))

        if isinstance(schema, str):
            if schema in self._named:
                return self.value(self._named[schema])
            return self._primitive(schema)

        if isinstance(schema, dict):
            kind = schema['type']
            if kind == 'record':
                self._named[schema['name']] = schema
                return {f['name']: self.value(f['type']) for f in schema['fields']}
            if kind == 'enum':
                self._named[schema['name']] = schema
                return random.choice(schema['symbols'])
            if kind == 'array':
                n = random.randint(0, 5)
                return [self.value(schema['items']) for _ in range(n)]
            return self._primitive(kind)

        raise ValueError(f'cannot generate value for: {schema!r}')

    @staticmethod
    def _primitive(name: str) -> object:
        if name == 'null':
            return None
        if name == 'boolean':
            return random.choice([True, False])
        if name == 'long':
            return random.randint(-(2**31), 2**31 - 1)
        if name == 'float':
            return random.uniform(-1e6, 1e6)
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

    try:
        parsed = fastavro.parse_schema(raw)
    except Exception as exc:  # pylint: disable=broad-exception-caught
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
    except Exception as exc:  # pylint: disable=broad-exception-caught
        sys.exit(f'stream_gen: {exc}')


if __name__ == '__main__':
    main()
