"""
Multi-Threaded Processing
=========================
Reads every CSV file in a directory concurrently using a thread pool.
The per-file processing logic is identical to process_single_threaded.py —
only the orchestration changes.

Note on the GIL: Python threads share one interpreter lock, so CPU-bound
work (pure number crunching) does NOT speed up with threads. However,
file I/O releases the GIL while waiting for the OS, so multiple threads
can read different files simultaneously and we do get a real speedup here.
"""

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from common import GlobalResult, print_summary, process_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def main(input_dir: Path, workers: int) -> None:
    csv_files = sorted(input_dir.glob("*.csv"))

    global_result = GlobalResult()
    start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file, path): path for path in csv_files}
        for future in as_completed(futures):
            global_result.file_results.append(future.result())

    elapsed = time.perf_counter() - start
    print_summary(global_result, elapsed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Multi-threaded processing of orders CSV files."
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
        help="number of threads to use",
    )
    args = parser.parse_args()

    main(input_dir=args.input_dir, workers=args.workers)
