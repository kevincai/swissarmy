#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import csv
import re
import sys
from typing import Dict, Iterable, TextIO

OUTPUT_COLUMNS = [
    "Timestamp",
    "Client",
    "State",
    "ErrorCode",
    "Time",
    "ScanBytes",
    "ScanRows",
    "ReturnRows",
    "CpuCostNs",
    "MemCostBytes",
    "QueryId",
    "IsQuery",
    "Digest",
]

COMPACT_COLUMNS = {
    "Timestamp",
    "Client",
    "State",
    "Time",
    "ScanBytes",
    "ScanRows",
    "ReturnRows",
    "CpuCostNs",
    "MemCostBytes",
    "QueryId",
}

ANY_LOG_START = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z \[[^\]]+\] ")
QUERY_LOG_START = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z \[query\] \|")
FIELD_PATTERNS = {
    key: re.compile(rf"\|{re.escape(key)}=(.*?)(?=\|[A-Za-z][A-Za-z0-9_]*=|$)", re.S)
    for key in OUTPUT_COLUMNS
}


def iter_query_records(input_stream: TextIO) -> Iterable[str]:
    current_lines = []
    for line in input_stream:
        if QUERY_LOG_START.match(line):
            if current_lines:
                yield "".join(current_lines)
            current_lines = [line]
            continue

        if ANY_LOG_START.match(line):
            if current_lines:
                yield "".join(current_lines)
                current_lines = []
            continue

        if current_lines:
            current_lines.append(line)

    if current_lines:
        yield "".join(current_lines)


def normalize_value(column: str, value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        return ""

    if column in COMPACT_COLUMNS:
        return re.sub(r"\s+", "", cleaned)

    return re.sub(r"\s*\n\s*", " ", cleaned)


def extract_columns(record: str) -> Dict[str, str]:
    result = {}
    for key, pattern in FIELD_PATTERNS.items():
        match = pattern.search(record)
        result[key] = normalize_value(key, match.group(1)) if match else ""
    return result


def open_input(path: str) -> TextIO:
    if path == "-":
        return sys.stdin
    return open(path, "r", encoding="utf-8", errors="replace")


def open_output(path: str) -> TextIO:
    if path == "-":
        return sys.stdout
    return open(path, "w", encoding="utf-8", newline="")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract selected fields from StarRocks query audit logs and output CSV."
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Input log file path, or '-' to read from stdin.",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Output CSV file path, or '-' to write to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_stream = open_input(args.input)
    output_stream = open_output(args.output)

    try:
        writer = csv.DictWriter(output_stream, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for record in iter_query_records(input_stream):
            writer.writerow(extract_columns(record))
    finally:
        if input_stream is not sys.stdin:
            input_stream.close()
        if output_stream is not sys.stdout:
            output_stream.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
