import argparse
import logging
import time
from pathlib import Path

from common import GlobalResult, print_summary, process_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def main(file_to_process: Path) -> None:
    global_result = GlobalResult()
    start = time.perf_counter()
    global_result.file_results.append(process_file(file_to_process))

    elapsed = time.perf_counter() - start
    print_summary(global_result, elapsed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Single-threaded processing of orders CSV files."
    )
    parser.add_argument(
        "--input-file",
        metavar="FILE",
        type=Path, required=True,
        help="file to process",
    )
    args = parser.parse_args()

    main(file_to_process=args.input_file)
