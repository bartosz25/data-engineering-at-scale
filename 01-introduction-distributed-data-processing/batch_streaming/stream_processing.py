import time
from pathlib import Path

from config import get_demo_dir_for_streaming


def load_all(path: Path) -> list[str]:
    print(f'Processing {path}')
    with path.open() as f:
        return [line.rstrip("\n") for line in f if line.strip()]


def watch_directory(directory: Path, poll_interval: float = 1.0):
    print(f'[STREAM] Watching {directory} for new .txt files...')
    seen: set[Path] = set()

    while True:
        current = set(directory.glob("*.txt"))
        for new_file in sorted(current - seen):
            yield new_file
        seen = current
        time.sleep(poll_interval)


def main() -> None:
    streaming_dir = Path(get_demo_dir_for_streaming())
    streaming_dir.mkdir(parents=True, exist_ok=True)

    for file_path in watch_directory(streaming_dir):
        records = load_all(file_path)
        for record in records:
            print(record)


if __name__ == "__main__":
    main()
