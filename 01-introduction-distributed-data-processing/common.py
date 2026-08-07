import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class FileResult:
    path: Path
    row_count: int = 0
    total_amount: float = 0.0
    min_amount: float = float("inf")
    max_amount: float = float("-inf")

    @property
    def avg_amount(self) -> float:
        return self.total_amount / self.row_count if self.row_count else 0.0


@dataclass
class GlobalResult:
    file_results: list[FileResult] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        return sum(r.row_count for r in self.file_results)

    @property
    def total_amount(self) -> float:
        return sum(r.total_amount for r in self.file_results)

    @property
    def avg_amount(self) -> float:
        return self.total_amount / self.row_count if self.row_count else 0.0

    @property
    def min_amount(self) -> float:
        return min(r.min_amount for r in self.file_results)

    @property
    def max_amount(self) -> float:
        return max(r.max_amount for r in self.file_results)


def process_file(path: Path) -> FileResult:
    result = FileResult(path=path)

    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            amount = float(row["order_amount"])
            result.row_count += 1
            result.total_amount += amount
            if amount < result.min_amount:
                result.min_amount = amount
            if amount > result.max_amount:
                result.max_amount = amount

    log.info(
        "%-40s  rows: %8s  total: $%12.2f  avg: $%8.2f",
        path.name, f"{result.row_count:,}",
        result.total_amount, result.avg_amount,
    )
    return result


def print_summary(global_result: GlobalResult, elapsed: float) -> None:
    sep = "-" * 60
    log.info(sep)
    log.info("files processed : %d", len(global_result.file_results))
    log.info("total rows      : %s", f"{global_result.row_count:,}")
    log.info("total revenue   : $%.2f", global_result.total_amount)
    log.info("avg order       : $%.2f", global_result.avg_amount)
    log.info("min order       : $%.2f", global_result.min_amount)
    log.info("max order       : $%.2f", global_result.max_amount)
    log.info("elapsed         : %.2f s", elapsed)
    log.info(sep)
