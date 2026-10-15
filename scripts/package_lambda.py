from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUILD_ROOT = PROJECT_ROOT / "build"
PACKAGE_DIR = BUILD_ROOT / "lambda"
OUTPUT_FILE = PROJECT_ROOT / "dist" / "relay-service.zip"


def main() -> None:
    reset_package_directory()
    PACKAGE_DIR.mkdir(parents=True)
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--target", str(PACKAGE_DIR), "."],
        cwd=PROJECT_ROOT,
        check=True,
    )
    write_archive()
    print(f"Created {OUTPUT_FILE.relative_to(PROJECT_ROOT)}")


def reset_package_directory() -> None:
    project_root = PROJECT_ROOT.resolve()
    package_dir = PACKAGE_DIR.resolve()
    if not package_dir.is_relative_to(project_root):
        raise RuntimeError("resolved package directory must remain inside the repository")
    output_file = OUTPUT_FILE.resolve()
    if not output_file.is_relative_to(project_root):
        raise RuntimeError("resolved Lambda archive must remain inside the repository")
    if PACKAGE_DIR.exists():
        shutil.rmtree(PACKAGE_DIR)


def write_archive() -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.unlink()
    with zipfile.ZipFile(OUTPUT_FILE, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in PACKAGE_DIR.rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
                archive.write(path, path.relative_to(PACKAGE_DIR))


if __name__ == "__main__":
    main()
