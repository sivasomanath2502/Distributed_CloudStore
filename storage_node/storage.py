import os
from pathlib import Path


STORAGE_DIR = Path(
    os.getenv("STORAGE_DIR", "data/node1")
)

STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def get_safe_path(file_id: str) -> Path:
    """
    Resolve a storage ID safely inside the storage directory.
    """

    if not file_id or Path(file_id).name != file_id:
        raise ValueError("Invalid storage ID")

    file_path = (STORAGE_DIR / file_id).resolve()

    if file_path.parent != STORAGE_DIR.resolve():
        raise ValueError("Invalid storage path")

    return file_path


def save_file(file_id: str, content: bytes):

    file_path = get_safe_path(file_id)

    with open(file_path, "wb") as file:
        file.write(content)

    return file_path


def get_file(file_id: str):

    file_path = get_safe_path(file_id)

    if not file_path.exists():
        return None

    with open(file_path, "rb") as file:
        return file.read()


def delete_file(file_id: str):

    file_path = get_safe_path(file_id)

    if file_path.exists():
        file_path.unlink()
        return True

    return False


def list_files():

    return [
        {
            "file_id": file.name,
            "size": file.stat().st_size
        }
        for file in STORAGE_DIR.iterdir()
        if file.is_file()
    ]