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


def main(input_dir: Path) -> None:
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        log.warning("no CSV files found in '%s'", input_dir)
        return

    log.info("found %d file(s) in '%s'", len(csv_files), input_dir)

    global_result = GlobalResult()
    start = time.perf_counter()

    for path in csv_files:
        global_result.file_results.append(process_file(path))

    elapsed = time.perf_counter() - start
    print_summary(global_result, elapsed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Single-threaded processing of orders CSV files."
    )
    parser.add_argument(
        "--input-dir",
        type=Path, required=True,
        metavar="DIR",
        help="directory containing CSV files to process",
    )
    args = parser.parse_args()

    main(input_dir=args.input_dir)
