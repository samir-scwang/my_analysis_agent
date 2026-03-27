#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import sys

EXCLUDE_DIRS = {".venv"}


def count_file_lines(file_path: Path) -> int:
    """统计单个文件总行数"""
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except UnicodeDecodeError:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            return sum(1 for _ in f)
    except Exception as e:
        print(f"跳过文件 {file_path}: {e}")
        return 0


def is_excluded(path: Path, root: Path) -> bool:
    """判断文件是否位于需要排除的目录中"""
    try:
        relative_parts = path.relative_to(root).parts
        return any(part in EXCLUDE_DIRS for part in relative_parts)
    except ValueError:
        return False


def list_py_file_lines(directory: Path):
    """递归列出目录下每个 .py 文件的行数，排除 .venv"""
    results = []
    for py_file in directory.rglob("*.py"):
        if py_file.is_file() and not is_excluded(py_file, directory):
            line_count = count_file_lines(py_file)
            results.append((py_file, line_count))
    return results


def main():
    if len(sys.argv) > 1:
        target_dir = Path(sys.argv[1]).resolve()
    else:
        target_dir = Path(".").resolve()

    if not target_dir.exists():
        print(f"目录不存在: {target_dir}")
        sys.exit(1)

    if not target_dir.is_dir():
        print(f"这不是一个目录: {target_dir}")
        sys.exit(1)

    results = list_py_file_lines(target_dir)
    results.sort(key=lambda x: str(x[0]))

    total = 0
    for file_path, line_count in results:
        relative_path = file_path.relative_to(target_dir)
        print(f"{relative_path}: {line_count}")
        total += line_count

    print("-" * 40)
    print(f"总行数: {total}")
    print(f"文件数: {len(results)}")


if __name__ == "__main__":
    main()