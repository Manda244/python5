#!/usr/bin/env python3
# ########################################################################### #
#   shebang: 1                                                                #
#                                                          :::      ::::::::  #
#   data_processor.py                                    :+:      :+:    :+:  #
#                                                      +:+ +:+         +:+    #
#   By: marasolo <marasolo@student.42antananarivo.   +#+  +:+       +#+       #
#                                                  +#+#+#+#+#+   +#+          #
#   Created: 2026/06/20 10:16:55 by marasolo            #+#    #+#            #
#   Updated: 2026/06/27 13:18:23 by marasolo           ###   ########.fr      #
#                                                                             #
# ########################################################################### #

from typing import Any, Sequence, Union
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise Exception("No data")

        value = self._storage.pop(0)
        ranks = self._rank
        self._rank += 1
        return ranks, value


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False

        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(items, (int, float))
                       and not isinstance(items, bool) for items in data)
        return False

    def ingest(self, data: Union[int, float,
                                 Sequence[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, (list, Sequence)) and not isinstance(data, str):
            for items in data:
                self._storage.append(str(items))
        else:
            self._storage.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(isinstance(items, str) for items in data)

        return False

    def ingest(self, data: Union[str, Sequence[str]]) -> None:
        if not self.validate(data):
            raise Exception("Improper text data")

        if isinstance(data, str):
            self._storage.append(data)
        else:
            for items in data:
                self._storage.append(items)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(k, str) and isinstance(v, str)
                       for k, v in data.items())

        if isinstance(data, list):
            return all(isinstance(items, dict) and all(isinstance(k, str) and
                       isinstance(v, str) for k, v in items.items())
                       for items in data)
        return False

    def ingest(self, data: Union[dict[str, str],
                                 Sequence[dict[str, str]]]) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        if isinstance(data, dict):
            entries = [data]
        else:
            entries = list(data)

        for entry in entries:
            level = entry.get("log_level", "")
            message = entry.get("log_message", "")
            self._storage.append(f"{level}: {message}")


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()
    print(f" Trying to validate input '42': {num_proc.validate(42)}")
    print(f" Trying to validate input 'Hello': {num_proc.validate("Hello")}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    input: Any = "foo"
    try:
        num_proc.ingest(input)
    except Exception as e:
        print(f" Got exception: {e}")
    data_num = [1, 2, 3, 4, 5]
    print(f" Processing data: {data_num}")
    num_proc.ingest(data_num)
    extract_count = 3
    print(f" Extracting {extract_count} values...")
    for _ in range(extract_count):
        ranks, values = num_proc.output()
        print(f" Numeric value {ranks}: {values}")

    print()
    print("Testing Text Processor...")
    text_proc = TextProcessor()
    print(f" Trying to validate input '42': {text_proc.validate(42)}")
    data_txt = ["Hello", "Nexus", "World"]
    print(f" Processing data: {data_txt}")
    text_proc.ingest(data_txt)
    extract_text = 1
    print(f" Extracting {extract_text} value...")
    ranks, values = text_proc.output()
    print(f" Text value {ranks}: {values}")

    print()
    print("Testing Log Processor...")
    log_proc = LogProcessor()
    print(f" Trying to validate input 'Hello': {log_proc.validate("Hello")}")
    data_log = [{"log_level": "NOTICE", "log_message": "Connection to server"},
                {"log_level": "ERROR", "log_message": "Unauthorized access!!"}]
    print(f" Processing data: {data_log}")
    log_proc.ingest(data_log)
    extract_log = 2
    print(f" Extracting {extract_log} values...")
    for _ in range(extract_log):
        ranks, values = log_proc.output()
        print(f" Log entry {ranks}: {values}")


if __name__ == "__main__":
    main()
