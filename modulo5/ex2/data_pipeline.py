from typing import Any, Union, Sequence
from abs import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list = []
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
        self._label = "Numeric processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False

        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(items, (int, float)) and not
                       isinstance(items , bool) for items in data)

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
        self._label = "Text processor"

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
        self._label = "Log processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return all(isinstance(k, str) and isinstance(v, str) for k, v in data.items())

        if isinstance(data, list):
            return all(isinstance(items, dict) and all(isinstance(k, str) and isinstance(v, str)
                       for k, v in items.items()) for items in data)

        return False

    def ingest(self, data: Union[dict[str,str]]) -> None:
        if not self.validate(data):
            raise Exception("Improper log data")

        for entry in (data if isinstance(data, list) else [data]):
            level = entry.get("Log_level", "")
            message = entry.get("Log_message", "")
            self._storage.append(f"{level}: {message}")
            self._total += 1


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
                print(f"Data strems error can't processor element in stream: {elements}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processor:
            print("No processor found, no data")
            return

        for proc in self._processor:
            print(f"{proc._label}: total {proc._total} remaining "
                  f"{len(proc._storage)} on processor")


def main() -> None:


if __name__ == "__main__"
    main()
