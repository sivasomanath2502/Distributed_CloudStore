import os
from pathlib import Path

STORAGE_DIR = Path(
    os.getenv("STORAGE_DIR", "data/node1")
)

STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def save_file(file_id: str, content: bytes):

    file_path = STORAGE_DIR / file_id

    with open(file_path, "wb") as file:
        file.write(content)

    return file_path


def get_file(file_id: str):

    file_path = STORAGE_DIR / file_id

    if not file_path.exists():
        return None

    with open(file_path, "rb") as file:
        return file.read()


def delete_file(file_id: str):

    file_path = STORAGE_DIR / file_id

    if file_path.exists():
        file_path.unlink()
        return True

    return False