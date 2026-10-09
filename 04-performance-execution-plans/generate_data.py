import argparse
import csv
import logging
import random
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SEED = 42
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

FILE_PREFIX = "orders_region"

START_DATE = date(2023, 1, 1)
END_DATE   = date.today()
DATE_RANGE = (END_DATE - START_DATE).days


def random_date() -> str:
    offset = random.randint(0, DATE_RANGE)
    return (START_DATE + timedelta(days=offset)).isoformat()


def random_amount() -> str:
    amount = random.uniform(1.0, 9_999.99)
    return f"{amount:.2f}"


def generate_file(path: Path, rows: int) -> None:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["first_name", "last_name", "order_date", "order_amount"])
        for _ in range(rows):
            writer.writerow([
                fake.first_name(),
                fake.last_name(),
                random_date(),
                random_amount(),
            ])
    log.info("created %s  (%s rows)", path, f"{rows:,}")


def main(rows_per_file: int, num_files: int, output_dir: Path) -> None:
    output_dir.mkdir(exist_ok=True)
    log.info(
        "generating %d files × %s rows into '%s/'",
        num_files, f"{rows_per_file:,}", output_dir,
    )

    for i in range(1, num_files + 1):
        path = output_dir / f"{FILE_PREFIX}_{i}.csv"
        generate_file(path, rows_per_file)

    log.info("done — total rows generated: %s", f"{num_files * rows_per_file:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic orders CSV files.")
    parser.add_argument(
        "--rows",
        type=int, required=True,
        metavar="N",
        help="number of rows per file",
    )
    parser.add_argument(
        "--num-files",
        type=int, required=True,
        metavar="N",
        help="number of CSV files to generate",
    )
    parser.add_argument(
        "--output-dir",
        type=Path, required=True,
        metavar="DIR",
        help="directory where CSV files will be written",
    )
    args = parser.parse_args()

    main(
        rows_per_file=args.rows,
        num_files=args.num_files,
        output_dir=args.output_dir,
    )
