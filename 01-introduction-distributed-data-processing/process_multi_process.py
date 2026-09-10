import argparse
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from common import FileResult, GlobalResult, print_summary, process_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _process_file(path: Path) -> FileResult:
    # Every worker has a *different* PID — each is a real OS process with its
    # own GIL. Watch the timestamps: multiple PIDs print START at roughly the
    # same wall-clock time, proving they run truly in parallel.
    pid = os.getpid()
    log.info("[PID-%d]  own GIL — START  %s", pid, path.name)
    t0 = time.perf_counter()
    result = process_file(path)
    log.info(
        "[PID-%d]  own GIL — END    %s  (%.2fs)",
        pid, path.name, time.perf_counter() - t0,
    )
    return result


def main(input_dir: Path, workers: int) -> None:
    csv_files = sorted(input_dir.glob("*.csv"))

    global_result = GlobalResult()
    start = time.perf_counter()

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_file, path): path for path in csv_files}
        for future in as_completed(futures):
            global_result.file_results.append(future.result())

    elapsed = time.perf_counter() - start
    print_summary(global_result, elapsed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Multi-process processing of orders CSV files."
    )
    parser.add_argument(
        "--input-dir",
        type=Path, required=True,
        metavar="DIR",
        help="directory containing CSV files to process",
    )
    parser.add_argument(
        "--workers",
        type=int, required=True,
        metavar="N",
        help="number of worker processes to use",
    )
    args = parser.parse_args()

    main(input_dir=args.input_dir, workers=args.workers)