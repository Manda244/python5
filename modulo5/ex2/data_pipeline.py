#!/usr/bin/env python3
# ########################################################################### #
#   shebang: 1                                                                #
#                                                          :::      ::::::::  #
#   data_pipeline.py                                     :+:      :+:    :+:  #
#                                                      +:+ +:+         +:+    #
#   By: marasolo <marasolo@student.42antananarivo.   +#+  +:+       +#+       #
#                                                  +#+#+#+#+#+   +#+          #
#   Created: 2026/06/27 09:38:58 by marasolo            #+#    #+#            #
#   Updated: 2026/06/27 13:16:20 by marasolo           ###   ########.fr      #
#                                                                             #
# ########################################################################### #

from typing import Any, Union, Sequence, Protocol
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._rank: int = 0
        self._total: int = 0
        self._label: str = ""

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise Exception("Not data")

        values = self._storage.pop(0)
        ranks = self._rank
        self._rank += 1
        return ranks, values


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self._label = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False

        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(items, (int, float)) and not
                       isinstance(items, bool) for items in data)

        return False

    def ingest(self, data: Union[int, float,
                                 Sequence[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise Exception("Improper numeric data")

        if isinstance(data, (list, Sequence)) and not isinstance(data, str):
            for items in data:
                self._storage.append(str(items))
                self._total += 1
        else:
            self._storage.append(str(data))
            self._total += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self._label = "Text Processor"

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
            self._total += 1
        else:
            for items in data:
                self._storage.append(items)
                self._total += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self._label = "Log Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(k, str) and
                       isinstance(v, str) for k, v in data.items())

        if isinstance(data, list):
            return all(isinstance(items, dict) and
                       all(isinstance(k, str) and isinstance(v, str)
                       for k, v in items.items()) for items in data)

        return False

    def ingest(self, data: Union[dict[str, str]]) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        for entry in (data if isinstance(data, list) else [data]):
            level = entry.get("log_level", "")
            message = entry.get("log_message", "")
            self._storage.append(f"{level}: {message}")
            self._total += 1


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CsvExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        line = ",".join(values for _, values in data)
        print(f"CSV Output: \n{line}")


class JsonExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pairs = ", ".join(f'"item_{ranks}": "{values}"'
                          for ranks, values in data)
        print(f"JSON Output: \n{{{pairs}}}")


class DataStream:
    def __init__(self) -> None:
        self._processor: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processor.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for elements in stream:
            handled = False
            for proc in self._processor:
                if proc.validate(elements):
                    proc.ingest(elements)
                    handled = True
                    break
            if not handled:
                print("Data strems error can't processor "
                      f"element in stream: {elements}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processor:
            print("No processor found, no data")
            return

        for proc in self._processor:
            print(f"{proc._label}: total {proc._total} items processed, remaining "
                  f"{len(proc._storage)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processor:
            count = min(nb, len(proc._storage))
            collected: list[tuple[int, str]] = []
            for _ in range(count):
                collected.append(proc.output())
            if collected:
                plugin.process_output(collected)


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print()
    print("Initialize Data Stream...")
    print()
    stream = DataStream()
    stream.print_processors_stats()
    print()
    print("Registering Processors")
    print()

    num_proc = NumericProcessor()
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(num_proc)
    stream.register_processor(txt_proc)
    stream.register_processor(log_proc)

    batch1: list[Any] = ["Hello world", [3.14, -1, 2.71], [{
                         "log_level": "WARNING",
                         "log_message": "Telnet access! Use ssh instead"},
                        {"log_level": "INFO",
                         "log_message": "User wil is connected"}],
                         42, ["Hi", "five"]]

    print(f"Send first batch of data on stream: {batch1}")
    print()

    stream.process_stream(batch1)
    stream.print_processors_stats()
    print()

    csv_plugin = CsvExportPlugin()
    print("Send 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, csv_plugin)
    print()

    stream.print_processors_stats()
    print()

    batch2: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [{"log_level": "ERROR",  "log_message": "500 server crash"},
         {"log_level": "NOTICE",
         "log_message": "Certificate expires in 10 days"}],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"Send another batch of data: {batch2}")
    print()

    stream.process_stream(batch2)
    stream.print_processors_stats()
    print()

    json_plugin = JsonExportPlugin()
    print("Send 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, json_plugin)
    print()

    stream.print_processors_stats()


if __name__ == "__main__":
    main()
