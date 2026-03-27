from __future__ import annotations

import sys
from pathlib import Path

from app.nodes.write_report import (
    DEFAULT_JSON_PATH,
    generate_report_stream_to_file,
    load_structured_result,
)


def main(argv: list[str]) -> int:
    json_path = Path(argv[1]).resolve() if len(argv) > 1 else DEFAULT_JSON_PATH
    data = load_structured_result(json_path)
    output_path = Path(argv[2]).resolve() if len(argv) > 2 else json_path.parent / "report.md"

    print(f"[info] input={json_path}")
    print(f"[info] output={output_path}")
    print("[info] 开始流式生成报告...\n")

    report_file = generate_report_stream_to_file(
        data,
        output_path,
        json_path=json_path,
        echo=True,
    )

    print(f"\n[OK] 报告已生成：{report_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
