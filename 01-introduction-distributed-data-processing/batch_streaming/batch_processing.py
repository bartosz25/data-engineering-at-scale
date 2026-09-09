import argparse
import time
from collections import defaultdict
from pathlib import Path

from config import get_demo_dir_for_batch


def load_all(path: Path) -> list[str]:
    print(f'Processing {path}')
    with path.open() as f:
        return [line.rstrip("\n") for line in f if line.strip()]

def main() -> None:
    batch_dir = Path(get_demo_dir_for_batch())
    batch_dir.mkdir(parents=True, exist_ok=True)
    print(f'Processing all files in {batch_dir}')
    text_files = sorted(batch_dir.glob("*.txt"))
    print(text_files)
    processed = 0
    for file_path in text_files:
        records = load_all(file_path)
        for record in records:
            print(record)
        processed += 1
    print(f'{processed} files have been processed')


if __name__ == "__main__":
    main()
