from __future__ import annotations

import html
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, cast

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage

from app.schemas.report import ReportDraft
from app.schemas.state import AnalysisGraphState
from app.services.dataframe_io import CSV_SUFFIXES, EXCEL_SUFFIXES, PARQUET_SUFFIXES, load_dataframe
from app.services.llm_service import LLMService


IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".svg", ".webp"}
TABLE_SUFFIXES = set(CSV_SUFFIXES) | set(EXCEL_SUFFIXES) | set(PARQUET_SUFFIXES)
DEFAULT_JSON_PATH = Path(
    r"E:\myagent\analysis_agent\app\artifacts\deepagent_runs\req_00\round_0\outputs\structured_result.json"
)
TOPIC_TITLE_MAP = {
    "overall_performance": "整体表现分析",
    "time_trend": "时间趋势分析",
    "regional_comparison": "区域对比分析",
    "geo_breakdown": "地理分布分析",
    "product_mix": "产品结构分析",
    "analysis_method": "分析方法说明",
    "misc": "补充分析",
}


llm = LLMService()


def load_structured_result(json_path: str | Path) -> Dict[str, Any]:
    path = Path(json_path)
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError(f"structured_result.json 根节点必须是对象: {path}")

    return data


def resolve_workspace_root(data: Dict[str, Any], json_path: str | Path) -> Path:
    run_metadata = data.get("run_metadata", {})
    if isinstance(run_metadata, dict):
        workspace_root = str(run_metadata.get("workspace_root", "")).strip()
        if workspace_root:
            return Path(workspace_root)

    json_file = Path(json_path).resolve()
    if json_file.parent.name.lower() == "outputs":
        return json_file.parent.parent
    return json_file.parent


def get_dataset_name(data: Dict[str, Any]) -> str:
    candidates = [
        data.get("dataset_path"),
        ((data.get("dataset_context") or {}).get("source_path") if isinstance(data.get("dataset_context"), dict) else None),
        ((data.get("run_metadata") or {}).get("dataset_path") if isinstance(data.get("run_metadata"), dict) else None),
    ]

    for candidate in candidates:
        if isinstance(candidate, str) and candidate.strip():
            return Path(candidate).name

    return "未知数据集"


def extract_topic_from_data(data: Dict[str, Any]) -> str:
    direct_candidates = [
        data.get("topic"),
        data.get("final_topic"),
        data.get("query"),
        data.get("title"),
        data.get("user_prompt"),
    ]
    for candidate in direct_candidates:
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()

    analysis_brief = data.get("analysis_brief", {})
    normalized_task = data.get("normalized_task", {})
    must_cover_topics: List[str] = []
    primary_questions: List[str] = []

    if isinstance(analysis_brief, dict):
        raw_topics = analysis_brief.get("must_cover_topics", [])
        if isinstance(raw_topics, list):
            must_cover_topics = [str(item).strip() for item in raw_topics if str(item).strip()]

    if isinstance(normalized_task, dict):
        raw_questions = normalized_task.get("primary_questions", [])
        if isinstance(raw_questions, list):
            primary_questions = [str(item).strip() for item in raw_questions if str(item).strip()]

    dataset_name = get_dataset_name(data)
    business_goal = ""
    if isinstance(normalized_task, dict):
        business_goal = str(normalized_task.get("business_goal", "")).strip()
    if not business_goal and isinstance(analysis_brief, dict):
        business_goal = str(analysis_brief.get("business_goal", "")).strip()

    parts = [f"基于 {dataset_name} 的数据分析报告"]
    if business_goal:
        parts.append(f"目标是{business_goal}")
    if must_cover_topics:
        parts.append("重点覆盖主题：" + "、".join(must_cover_topics))
    if primary_questions:
        parts.append("核心问题包括：" + "、".join(primary_questions))

    return "。".join(parts) + "。"


def format_dataset_label(data: Dict[str, Any]) -> str:
    dataset_name = Path(get_dataset_name(data)).stem
    return dataset_name.replace("_", " ").replace("-", " ").strip() or "数据集"


def slug_to_title(text: str) -> str:
    cleaned = text.replace("_", " ").replace("-", " ").strip()
    return " ".join(part.capitalize() for part in cleaned.split()) or "Untitled"


def infer_topic_tags_from_name(name: str) -> List[str]:
    lower = name.lower()
    tags: List[str] = []

    if any(token in lower for token in ["summary", "kpi", "overall", "profit_summary"]):
        tags.append("overall_performance")
    if any(token in lower for token in ["trend", "time", "date", "daily"]):
        tags.append("time_trend")
    if any(token in lower for token in ["region", "regional", "geo", "country", "reporter", "partner"]):
        tags.append("regional_comparison")
    if any(token in lower for token in ["product", "category", "mix", "sku"]):
        tags.append("product_mix")
    if any(token in lower for token in ["method", "script", "calculation", "code"]):
        tags.append("analysis_method")

    return tags or ["misc"]


def normalize_artifact(item: Dict[str, Any]) -> Dict[str, Any] | None:
    path = Path(str(item.get("path", "")).strip())
    if not path:
        return None

    resolved_path = path.resolve()
    if not resolved_path.exists():
        return None

    artifact_type = str(item.get("artifact_type", "")).strip().lower()
    if not artifact_type:
        if resolved_path.suffix.lower() in IMAGE_SUFFIXES:
            artifact_type = "chart"
        elif resolved_path.suffix.lower() in TABLE_SUFFIXES:
            artifact_type = "table"
        else:
            artifact_type = "file"

    title = str(item.get("title", "")).strip() or slug_to_title(resolved_path.stem)
    topic_tags = item.get("topic_tags", [])
    if not isinstance(topic_tags, list) or not topic_tags:
        topic_tags = infer_topic_tags_from_name(f"{title} {resolved_path.stem}")

    return {
        "artifact_id": str(item.get("artifact_id", "")).strip() or resolved_path.stem,
        "artifact_type": artifact_type,
        "title": title,
        "path": str(resolved_path),
        "format": str(item.get("format", "")).strip() or resolved_path.suffix.lstrip(".").lower(),
        "topic_tags": [str(tag).strip() for tag in topic_tags if str(tag).strip()],
    }


def scan_workspace_artifacts(workspace_root: Path) -> List[Dict[str, Any]]:
    artifacts: List[Dict[str, Any]] = []

    for folder_name, artifact_type, suffixes in [
        ("tables", "table", TABLE_SUFFIXES),
        ("charts", "chart", IMAGE_SUFFIXES),
    ]:
        folder = workspace_root / folder_name
        if not folder.exists():
            continue

        for path in sorted(folder.iterdir()):
            if not path.is_file() or path.suffix.lower() not in suffixes:
                continue

            artifacts.append(
                {
                    "artifact_id": path.stem,
                    "artifact_type": artifact_type,
                    "title": slug_to_title(path.stem),
                    "path": str(path.resolve()),
                    "format": path.suffix.lstrip(".").lower(),
                    "topic_tags": infer_topic_tags_from_name(path.stem),
                }
            )

    return artifacts


def collect_artifacts(data: Dict[str, Any], json_path: str | Path) -> List[Dict[str, Any]]:
    raw_artifacts = data.get("artifacts", [])
    normalized: List[Dict[str, Any]] = []

    if isinstance(raw_artifacts, list):
        for item in raw_artifacts:
            if not isinstance(item, dict):
                continue
            normalized_item = normalize_artifact(item)
            if normalized_item is not None and normalized_item["artifact_type"] in {"table", "chart"}:
                normalized.append(normalized_item)

    if normalized:
        return normalized

    workspace_root = resolve_workspace_root(data, json_path)
    return scan_workspace_artifacts(workspace_root)


def group_artifacts_by_topic(artifacts: List[Dict[str, Any]]) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    grouped: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for artifact in artifacts:
        artifact_type = str(artifact.get("artifact_type", "")).strip().lower()
        topic_tags = artifact.get("topic_tags", [])
        if not isinstance(topic_tags, list) or not topic_tags:
            topic_tags = ["misc"]

        for topic_tag in topic_tags:
            normalized = str(topic_tag).strip() or "misc"
            grouped.setdefault(normalized, {"table": [], "chart": []})
            if artifact_type == "table":
                grouped[normalized]["table"].append(artifact)
            elif artifact_type == "chart":
                grouped[normalized]["chart"].append(artifact)

    return grouped


def get_topic_order(data: Dict[str, Any], grouped: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> List[str]:
    ordered: List[str] = []

    analysis_brief = data.get("analysis_brief", {})
    if isinstance(analysis_brief, dict):
        raw_topics = analysis_brief.get("must_cover_topics", [])
        if isinstance(raw_topics, list):
            for topic in raw_topics:
                normalized = str(topic).strip()
                if normalized and normalized not in ordered:
                    ordered.append(normalized)

    for topic in grouped:
        if topic not in ordered:
            ordered.append(topic)

    return ordered


def extract_title_themes(data: Dict[str, Any], artifacts: List[Dict[str, Any]]) -> List[str]:
    themes: List[str] = []

    analysis_brief = data.get("analysis_brief", {})
    if isinstance(analysis_brief, dict):
        raw_topics = analysis_brief.get("must_cover_topics", [])
        if isinstance(raw_topics, list):
            for topic in raw_topics:
                label = TOPIC_TITLE_MAP.get(str(topic).strip(), str(topic).strip())
                if label and label not in themes:
                    themes.append(label)

    if not themes:
        grouped = group_artifacts_by_topic(artifacts)
        for topic in grouped:
            label = TOPIC_TITLE_MAP.get(topic, topic)
            if label and label not in themes:
                themes.append(label)

    if not themes:
        normalized_task = data.get("normalized_task", {})
        if isinstance(normalized_task, dict):
            questions = normalized_task.get("primary_questions", [])
            if isinstance(questions, list):
                for item in questions:
                    label = str(item).strip()
                    if label and label not in themes:
                        themes.append(label)

    return themes[:3]


def build_fallback_report_title(data: Dict[str, Any], artifacts: List[Dict[str, Any]]) -> str:
    dataset_label = format_dataset_label(data)
    themes = extract_title_themes(data, artifacts)
    if themes:
        return f"{dataset_label}：{('、'.join(themes))}分析报告"
    return f"{dataset_label}数据分析报告"


def generate_report_title(topic: str, data: Dict[str, Any], artifacts: List[Dict[str, Any]]) -> str:
    fallback_title = build_fallback_report_title(data, artifacts)
    theme_text = "、".join(extract_title_themes(data, artifacts))

    try:
        response = llm.invoke(
            [
                SystemMessage(
                    content=(
                        "你是一名专业的中文报告标题撰写助手。"
                        "请根据给定的报告主题、数据集名称和分析主题生成一个正式、简洁、适合作为 Markdown 一级标题的中文标题。"
                        "标题必须体现分析主题。"
                        "不要输出解释，不要输出编号，不要输出 markdown 符号，不要换行。"
                    )
                ),
                HumanMessage(
                    content=(
                        f"报告主题如下：\n{topic}\n\n"
                        f"数据集名称：{format_dataset_label(data)}\n"
                        f"需要体现的分析主题：{theme_text or '综合分析'}\n"
                        f"可参考的兜底标题：{fallback_title}"
                    )
                ),
            ]
        )
        title = (response.content or "").replace("#", "").strip()
        return title or fallback_title
    except Exception:
        return fallback_title


def build_report_header(title: str) -> str:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return f"# {title}\n\n**撰写时间：{now_str}**\n"


def rewrite_report_title(report_file: Path, final_title: str) -> None:
    content = report_file.read_text(encoding="utf-8")
    marker = "\n---\n\n"
    if marker not in content:
        report_file.write_text(build_report_header(final_title), encoding="utf-8")
        return

    _, rest = content.split(marker, 1)
    report_file.write_text(build_report_header(final_title) + marker + rest, encoding="utf-8")


def read_table_artifact(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"找不到表格文件: {path}")
    return load_dataframe(path)


def dataframe_to_centered_html_table(df: pd.DataFrame, max_rows: int = 15) -> str:
    if df.empty:
        return '<p align="center"><em>表格为空</em></p>'

    preview_df = df.head(max_rows).copy().fillna("")
    for col in preview_df.columns:
        preview_df[col] = preview_df[col].map(
            lambda value: f"{value:.4g}" if isinstance(value, float) else str(value)
        )

    headers = "".join(f"<th>{html.escape(str(col))}</th>" for col in preview_df.columns)
    rows: List[str] = []
    for row in preview_df.itertuples(index=False, name=None):
        cells = "".join(f"<td>{html.escape(str(value))}</td>" for value in row)
        rows.append(f"<tr>{cells}</tr>")

    return (
        '<div align="center">\n'
        '<table>\n'
        f"<thead><tr>{headers}</tr></thead>\n"
        f"<tbody>{''.join(rows)}</tbody>\n"
        "</table>\n"
        "</div>"
    )


def resolve_asset_reference(asset_path: str, report_path: str | Path) -> str:
    path = Path(asset_path).resolve()
    report_dir = Path(report_path).resolve().parent
    try:
        return os.path.relpath(path, report_dir).replace("\\", "/")
    except ValueError:
        return path.as_uri()


def build_centered_table_block(*, title: str, table_number: int, df: pd.DataFrame) -> str:
    safe_title = html.escape(title)
    return (
        '<div align="center">\n'
        f"<p><strong>表 {table_number}</strong> {safe_title}</p>\n"
        "</div>\n"
        f"{dataframe_to_centered_html_table(df)}"
    )


def build_centered_image_block(
    *,
    image_path: str,
    title: str,
    figure_number: int,
    report_path: str | Path,
) -> str:
    path = Path(image_path).resolve()
    if not path.exists():
        return f"_图表文件不存在：{path}_"

    ref = resolve_asset_reference(str(path), report_path)
    safe_title = html.escape(title)
    return (
        '<div align="center">\n'
        f'<img src="{html.escape(ref)}" alt="{safe_title}" style="max-width: 100%; height: auto;" />\n'
        f"<p><strong>图 {figure_number}</strong> {safe_title}</p>\n"
        "</div>"
    )


def find_related_table_for_chart(
    chart_artifact: Dict[str, Any],
    topic_tables: List[Dict[str, Any]],
) -> Dict[str, Any] | None:
    chart_stem = Path(str(chart_artifact.get("path", ""))).stem.lower()
    chart_tokens = {token for token in chart_stem.replace("-", "_").split("_") if token}

    best_match: Dict[str, Any] | None = None
    best_score = -1
    for table_artifact in topic_tables:
        table_stem = Path(str(table_artifact.get("path", ""))).stem.lower()
        table_tokens = {token for token in table_stem.replace("-", "_").split("_") if token}
        score = len(chart_tokens.intersection(table_tokens))
        if score > best_score:
            best_match = table_artifact
            best_score = score

    return best_match if best_score > 0 else (topic_tables[0] if topic_tables else None)


def build_summary_context(data: Dict[str, Any], artifacts: List[Dict[str, Any]]) -> str:
    dataset_context = data.get("dataset_context", {})
    analysis_brief = data.get("analysis_brief", {})
    normalized_task = data.get("normalized_task", {})

    pieces: List[str] = [f"数据集：{get_dataset_name(data)}"]

    if isinstance(normalized_task, dict):
        business_goal = str(normalized_task.get("business_goal", "")).strip()
        if business_goal:
            pieces.append(f"业务目标：{business_goal}")

    if isinstance(analysis_brief, dict):
        must_cover_topics = analysis_brief.get("must_cover_topics", [])
        if isinstance(must_cover_topics, list) and must_cover_topics:
            pieces.append("必须覆盖主题：" + "、".join(str(item) for item in must_cover_topics))

    if isinstance(dataset_context, dict):
        tables = dataset_context.get("tables", [])
        if isinstance(tables, list) and tables:
            first_table = tables[0]
            if isinstance(first_table, dict):
                pieces.append(f"数据规模：{first_table.get('row_count')} 行，{first_table.get('column_count')} 列")

        time_coverage = dataset_context.get("time_coverage", {})
        if isinstance(time_coverage, dict):
            start = time_coverage.get("min")
            end = time_coverage.get("max")
            if start or end:
                pieces.append(f"时间覆盖：{start} 至 {end}")

        business_hints = dataset_context.get("business_hints", [])
        if isinstance(business_hints, list) and business_hints:
            pieces.append("业务提示：" + "；".join(str(item) for item in business_hints[:4]))

    findings = data.get("findings", [])
    if isinstance(findings, list) and findings:
        lines = []
        for item in findings[:4]:
            if isinstance(item, dict):
                title = str(item.get("title") or item.get("finding") or "").strip()
                desc = str(item.get("description") or item.get("statement") or "").strip()
                if title or desc:
                    lines.append(f"- {title}：{desc}".strip("："))
        if lines:
            pieces.append("关键发现：\n" + "\n".join(lines))

    claims = data.get("claims", [])
    if isinstance(claims, list) and claims:
        lines = []
        for item in claims[:3]:
            if isinstance(item, dict):
                claim = str(item.get("claim") or item.get("claim_text") or "").strip()
                confidence = str(item.get("confidence", "")).strip()
                if claim:
                    suffix = f"（置信度：{confidence}）" if confidence else ""
                    lines.append(f"- {claim}{suffix}")
        if lines:
            pieces.append("核心结论：\n" + "\n".join(lines))

    pieces.append(f"可用图表与表格数量：{len(artifacts)}")
    return "\n\n".join(pieces)


def infer_domain_background(data: Dict[str, Any]) -> str:
    dataset_name = get_dataset_name(data).lower()
    dataset_context = data.get("dataset_context", {})
    columns: List[str] = []
    if isinstance(dataset_context, dict):
        tables = dataset_context.get("tables", [])
        if isinstance(tables, list):
            for table in tables[:1]:
                if isinstance(table, dict):
                    raw_columns = table.get("columns", [])
                    if isinstance(raw_columns, list):
                        columns.extend(
                            str(col.get("name", "")).strip().lower()
                            for col in raw_columns
                            if isinstance(col, dict)
                        )

    text = " ".join([dataset_name, *columns])
    if any(token in text for token in ["caloric", "export", "reporter", "partner", "food", "trade"]):
        return (
            "该数据反映跨国食品或农产品贸易在热量口径下的流动情况，"
            "常用于观察全球供给结构、主要出口方与进口方的集中度，以及国际贸易网络中的区域分工。"
        )
    if any(token in text for token in ["sale", "sales", "order", "gmv", "cost", "region", "category"]):
        return (
            "该数据属于典型的经营分析明细数据，通常用于评估销售规模、时间趋势、区域差异、"
            "产品结构以及成本与收益之间的关系，为经营决策和资源配置提供依据。"
        )
    return (
        "该数据属于结构化业务分析数据，适合从时间、维度、指标和数据质量多个角度建立背景认识，"
        "再进一步展开描述性和对比性分析。"
    )


def build_introduction_context(data: Dict[str, Any], artifacts: List[Dict[str, Any]]) -> str:
    dataset_context = data.get("dataset_context", {})
    normalized_task = data.get("normalized_task", {})
    analysis_brief = data.get("analysis_brief", {})

    pieces: List[str] = [f"数据集名称：{get_dataset_name(data)}", f"背景知识：{infer_domain_background(data)}"]

    if isinstance(normalized_task, dict):
        business_goal = str(normalized_task.get("business_goal", "")).strip()
        audience = str(normalized_task.get("target_audience", "")).strip()
        if business_goal:
            pieces.append(f"分析目标：{business_goal}")
        if audience:
            pieces.append(f"目标读者：{audience}")

    if isinstance(dataset_context, dict):
        measures = dataset_context.get("candidate_measure_columns", [])
        dimensions = dataset_context.get("candidate_dimension_columns", [])
        time_columns = dataset_context.get("candidate_time_columns", [])
        if isinstance(measures, list) and measures:
            pieces.append("核心指标：" + "、".join(str(item) for item in measures))
        if isinstance(dimensions, list) and dimensions:
            pieces.append("关键维度：" + "、".join(str(item) for item in dimensions))
        if isinstance(time_columns, list) and time_columns:
            pieces.append("时间字段：" + "、".join(str(item) for item in time_columns))

        time_coverage = dataset_context.get("time_coverage", {})
        if isinstance(time_coverage, dict):
            start = str(time_coverage.get("min", "")).strip()
            end = str(time_coverage.get("max", "")).strip()
            granularity = time_coverage.get("granularity_candidates", [])
            if start or end:
                pieces.append(f"时间覆盖范围：{start} 至 {end}".strip())
            if isinstance(granularity, list) and granularity:
                pieces.append("时间粒度候选：" + "、".join(str(item) for item in granularity))

        hints = dataset_context.get("business_hints", [])
        if isinstance(hints, list) and hints:
            pieces.append("数据提示：" + "；".join(str(item) for item in hints[:5]))

    if isinstance(analysis_brief, dict):
        topics = analysis_brief.get("must_cover_topics", [])
        if isinstance(topics, list) and topics:
            pieces.append("重点分析主题：" + "、".join(str(item) for item in topics))

    pieces.append(f"当前可引用图表和表格数量：{len(artifacts)}")
    return "\n\n".join(pieces)


def filter_topic_items(items: Any, topic_tag: str) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        return []

    result: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        item_topic = str(item.get("topic", "")).strip()
        topic_tags = item.get("topic_tags", [])
        if item_topic == topic_tag:
            result.append(item)
            continue
        if isinstance(topic_tags, list) and topic_tag in {str(tag).strip() for tag in topic_tags}:
            result.append(item)
    return result


def build_topic_context(
    data: Dict[str, Any],
    topic_tag: str,
    topic_tables: List[Dict[str, Any]],
    topic_charts: List[Dict[str, Any]],
) -> str:
    findings = filter_topic_items(data.get("findings", []), topic_tag)
    claims = filter_topic_items(data.get("claims", []), topic_tag)

    pieces = [f"主题标签：{topic_tag}", f"表格数量：{len(topic_tables)}，图表数量：{len(topic_charts)}"]

    if findings:
        lines = []
        for item in findings[:4]:
            title = str(item.get("title") or item.get("finding") or "").strip()
            desc = str(item.get("description") or item.get("statement") or "").strip()
            if title or desc:
                lines.append(f"- {title}：{desc}".strip("："))
        if lines:
            pieces.append("相关发现：\n" + "\n".join(lines))

    if claims:
        lines = []
        for item in claims[:4]:
            claim = str(item.get("claim") or item.get("claim_text") or "").strip()
            if claim:
                lines.append(f"- {claim}")
        if lines:
            pieces.append("相关结论：\n" + "\n".join(lines))

    return "\n\n".join(pieces)


def build_limitations_context(data: Dict[str, Any]) -> tuple[str, List[Dict[str, Any]]]:
    caveats = data.get("caveats", [])
    if isinstance(caveats, list) and caveats:
        normalized = []
        for idx, item in enumerate(caveats, start=1):
            if isinstance(item, str):
                normalized.append({"id": f"caveat_{idx:03d}", "message": item, "severity": "medium"})
            elif isinstance(item, dict):
                normalized.append(
                    {
                        "id": str(item.get("caveat_id") or item.get("id") or f"caveat_{idx:03d}"),
                        "message": str(item.get("message") or item.get("description") or item.get("content") or "").strip(),
                        "severity": str(item.get("severity", "medium")).strip() or "medium",
                    }
                )
        if normalized:
            return "限制信息来自结构化输出中的 caveats。", normalized

    dataset_context = data.get("dataset_context", {})
    if isinstance(dataset_context, dict):
        quality = dataset_context.get("data_quality_summary", {})
        if isinstance(quality, dict):
            missingness = quality.get("missingness", [])
            if isinstance(missingness, list) and missingness:
                normalized = []
                for idx, item in enumerate(missingness, start=1):
                    if not isinstance(item, dict):
                        continue
                    normalized.append(
                        {
                            "id": f"missing_{idx:03d}",
                            "message": f"{item.get('column', '')} 存在缺失值",
                            "severity": "medium",
                            "null_ratio": item.get("null_ratio"),
                        }
                    )
                if normalized:
                    return "限制信息来自 dataset_context.data_quality_summary.missingness。", normalized

    return "", []


def build_section_intro_messages(topic: str, section_title: str, topic_context: str) -> List[Any]:
    return [
        SystemMessage(
            content=(
                "你是一名专业的中文数据分析报告撰写助手。"
                "请为报告章节撰写一段自然的开场说明。"
                "使用正式、清晰、连贯的中文段落。"
                "不要输出二级标题，不要写成列表。"
            )
        ),
        HumanMessage(
            content=(
                f"报告主题：{topic}\n"
                f"章节标题：{section_title}\n\n"
                f"章节上下文如下：\n{topic_context}"
            )
        ),
    ]


def build_table_analysis_messages(topic: str, section_title: str, table_title: str, df: pd.DataFrame) -> List[Any]:
    csv_preview = df.head(15).to_csv(index=False)
    return [
        SystemMessage(
            content=(
                "你是一名专业的中文数据分析报告撰写助手。"
                "现在需要针对一张表格撰写分析说明。"
                "使用正式、自然、连贯的中文段落，解释表格反映的主要信息和关键特征。"
                "不要输出标题，不要写成列表。"
            )
        ),
        HumanMessage(
            content=(
                f"报告主题：{topic}\n"
                f"章节标题：{section_title}\n"
                f"表格标题：{table_title}\n\n"
                f"表格预览数据如下：\n{csv_preview}"
            )
        ),
    ]


def build_chart_analysis_messages(topic: str, section_title: str, chart_title: str, related_table_preview: str) -> List[Any]:
    return [
        SystemMessage(
            content=(
                "你是一名专业的中文数据分析报告撰写助手。"
                "现在需要针对一张图表撰写解读说明。"
                "使用正式、自然、连贯的中文段落解释图表揭示的主要趋势或对比关系。"
                "不要输出标题，不要写成列表。"
            )
        ),
        HumanMessage(
            content=(
                f"报告主题：{topic}\n"
                f"章节标题：{section_title}\n"
                f"图表标题：{chart_title}\n\n"
                f"相关表格预览如下：\n{related_table_preview}"
            )
        ),
    ]


def build_executive_summary_messages(topic: str, summary_context: str) -> List[Any]:
    return [
        SystemMessage(
            content=(
                "你是一名专业的中文数据分析报告撰写助手。"
                "请撰写“执行摘要”正文。"
                "要求使用正式、简洁、清晰的中文自然段。"
                "概括数据范围、分析重点、主要发现和业务含义。"
                "不要输出标题，不要写成列表。"
            )
        ),
        HumanMessage(content=f"报告主题：{topic}\n\n背景信息如下：\n{summary_context}"),
    ]


def build_introduction_messages(topic: str, introduction_context: str) -> List[Any]:
    return [
        SystemMessage(
            content=(
                "你是一名专业的中文数据分析报告撰写助手。"
                "请撰写报告的“引言”章节。"
                "要求先根据数据所属领域介绍必要的背景知识，再说明本次数据的分析范围、核心指标、关键维度、报告目标与阅读价值。"
                "使用正式、清晰、连贯的中文自然段。"
                "不要输出标题，不要写成列表。"
            )
        ),
        HumanMessage(content=f"报告主题：{topic}\n\n引言背景如下：\n{introduction_context}"),
    ]


def build_limitations_messages(topic: str, limitation_context: str, table_preview: str) -> List[Any]:
    return [
        SystemMessage(
            content=(
                "你是一名专业的中文报告撰写助手。"
                "请撰写“分析限制与说明”正文。"
                "要求使用正式、清晰、连贯的中文自然段。"
                "说明这些限制会如何影响结论理解和使用边界。"
                "不要输出标题，不要写成列表。"
            )
        ),
        HumanMessage(
            content=(
                f"报告主题：{topic}\n"
                f"限制背景：{limitation_context}\n\n"
                f"限制信息预览如下：\n{table_preview}"
            )
        ),
    ]


def build_conclusion_messages(topic: str, data: Dict[str, Any]) -> List[Any]:
    context = {
        "findings": data.get("findings", []),
        "claims": data.get("claims", []),
        "analysis_brief": data.get("analysis_brief", {}),
    }
    return [
        SystemMessage(
            content=(
                "你是一名专业的中文报告撰写助手。"
                "请撰写“结论与建议”正文。"
                "要求使用正式、自然、连贯的中文自然段。"
                "总结核心发现，指出业务启示，并给出谨慎、可执行的建议。"
                "不要输出标题，不要写成列表。"
            )
        ),
        HumanMessage(content=f"报告主题：{topic}\n\n结构化分析信息如下：\n{json.dumps(context, ensure_ascii=False)}"),
    ]


def write_and_echo(handle, text: str, *, echo: bool = True) -> None:
    handle.write(text)
    handle.flush()
    if echo:
        sys.stdout.write(text)
        sys.stdout.flush()


def format_chinese_paragraphs(text: str) -> str:
    lines = text.replace("\r\n", "\n").split("\n")
    output: List[str] = []
    paragraph: List[str] = []

    def flush_paragraph() -> None:
        if not paragraph:
            return
        merged = " ".join(part.strip() for part in paragraph if part.strip())
        if merged:
            output.append(f"　　{merged}")
        paragraph.clear()

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped:
            flush_paragraph()
            if output and output[-1] != "":
                output.append("")
            continue

        if stripped.startswith(("#", "|", "<", "![", "```")):
            flush_paragraph()
            output.append(raw_line)
            continue

        if stripped.startswith(("_", "-", "*")) and stripped.endswith("_"):
            flush_paragraph()
            output.append(raw_line)
            continue

        paragraph.append(stripped)

    flush_paragraph()
    while output and output[-1] == "":
        output.pop()
    return "\n".join(output)


def stream_model_to_report(handle, messages: Iterable[Any], *, echo: bool = True) -> str:
    chunks: List[str] = []
    for chunk in llm.stream_invoke(list(messages)):
        chunks.append(chunk)
    content = format_chinese_paragraphs("".join(chunks))
    if content:
        write_and_echo(handle, content, echo=echo)
        if not content.endswith("\n"):
            write_and_echo(handle, "\n", echo=echo)
    return content


def stream_topic_section(
    handle,
    *,
    section_number: int,
    topic: str,
    topic_tag: str,
    topic_tables: List[Dict[str, Any]],
    topic_charts: List[Dict[str, Any]],
    data: Dict[str, Any],
    report_path: str | Path,
    counters: Dict[str, int],
    echo: bool = True,
) -> None:
    section_title = TOPIC_TITLE_MAP.get(topic_tag, f"{topic_tag}分析")
    write_and_echo(handle, f"## {section_number}. {section_title}\n\n", echo=echo)
    topic_context = build_topic_context(data, topic_tag, topic_tables, topic_charts)
    stream_model_to_report(handle, build_section_intro_messages(topic, section_title, topic_context), echo=echo)
    write_and_echo(handle, "\n", echo=echo)

    for table_artifact in topic_tables:
        table_title = str(table_artifact.get("title", "未命名表格"))
        table_path = str(table_artifact.get("path", "")).strip()
        try:
            df = read_table_artifact(table_path)
            counters["table"] += 1
            write_and_echo(
                handle,
                "\n" + build_centered_table_block(title=table_title, table_number=counters["table"], df=df) + "\n\n",
                echo=echo,
            )
            stream_model_to_report(handle, build_table_analysis_messages(topic, section_title, table_title, df), echo=echo)
        except Exception as exc:
            write_and_echo(handle, f"_读取表格失败：{exc}_\n", echo=echo)
        write_and_echo(handle, "\n", echo=echo)

    for chart_artifact in topic_charts:
        chart_title = str(chart_artifact.get("title", "未命名图表"))
        chart_path = str(chart_artifact.get("path", "")).strip()
        related_table = find_related_table_for_chart(chart_artifact, topic_tables)
        related_preview = "无可用相关表格"

        if related_table is not None:
            try:
                related_df = read_table_artifact(str(related_table.get("path", "")))
                related_preview = related_df.head(10).to_csv(index=False)
            except Exception:
                related_preview = "相关表格读取失败"

        counters["figure"] += 1
        write_and_echo(
            handle,
            "\n"
            + build_centered_image_block(
                image_path=chart_path,
                title=chart_title,
                figure_number=counters["figure"],
                report_path=report_path,
            )
            + "\n\n",
            echo=echo,
        )
        stream_model_to_report(handle, build_chart_analysis_messages(topic, section_title, chart_title, related_preview), echo=echo)
        write_and_echo(handle, "\n", echo=echo)


def generate_report_stream_to_file(
    data: Dict[str, Any],
    report_path: str | Path,
    *,
    json_path: str | Path | None = None,
    echo: bool = True,
) -> Path:
    topic = extract_topic_from_data(data)
    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)

    artifacts = collect_artifacts(data, json_path or report_file)
    grouped = group_artifacts_by_topic(artifacts)
    topic_order = get_topic_order(data, grouped)
    summary_context = build_summary_context(data, artifacts)
    introduction_context = build_introduction_context(data, artifacts)
    limitation_context, normalized_limitations = build_limitations_context(data)
    counters = {"table": 0, "figure": 0}
    section_number = 1

    with report_file.open("w", encoding="utf-8") as handle:
        write_and_echo(handle, build_report_header("数据分析报告（生成中）"), echo=echo)
        write_and_echo(handle, "\n---\n\n", echo=echo)
        write_and_echo(handle, f"## {section_number}. 引言\n\n", echo=echo)
        stream_model_to_report(handle, build_introduction_messages(topic, introduction_context), echo=echo)
        section_number += 1

        write_and_echo(handle, "\n---\n\n", echo=echo)
        write_and_echo(handle, f"## {section_number}. 执行摘要\n\n", echo=echo)
        stream_model_to_report(handle, build_executive_summary_messages(topic, summary_context), echo=echo)
        section_number += 1

        for topic_tag in topic_order:
            payload = grouped.get(topic_tag, {"table": [], "chart": []})
            if not payload["table"] and not payload["chart"] and not filter_topic_items(data.get("findings", []), topic_tag):
                continue

            write_and_echo(handle, "\n---\n\n", echo=echo)
            stream_topic_section(
                handle,
                section_number=section_number,
                topic=topic,
                topic_tag=topic_tag,
                topic_tables=payload["table"],
                topic_charts=payload["chart"],
                data=data,
                report_path=report_file,
                counters=counters,
                echo=echo,
            )
            section_number += 1

        if normalized_limitations:
            limitation_df = pd.DataFrame(normalized_limitations)
            counters["table"] += 1
            write_and_echo(handle, "\n---\n\n", echo=echo)
            write_and_echo(handle, f"## {section_number}. 分析限制与说明\n\n", echo=echo)
            write_and_echo(
                handle,
                build_centered_table_block(
                    title="限制事项汇总表",
                    table_number=counters["table"],
                    df=limitation_df.head(20),
                )
                + "\n\n",
                echo=echo,
            )
            stream_model_to_report(
                handle,
                build_limitations_messages(topic, limitation_context, limitation_df.head(20).to_csv(index=False)),
                echo=echo,
            )
            section_number += 1

        write_and_echo(handle, "\n---\n\n", echo=echo)
        write_and_echo(handle, f"## {section_number}. 结论与建议\n\n", echo=echo)
        stream_model_to_report(handle, build_conclusion_messages(topic, data), echo=echo)

    rewrite_report_title(report_file, generate_report_title(topic, data, artifacts))
    return report_file


def _infer_table_topic_tags(table: Dict[str, Any]) -> List[str]:
    title = str(table.get("title", "")).lower()
    table_type = str(table.get("table_type", "")).lower()
    raw = f"{title} {table_type}"

    if any(token in raw for token in ["summary", "kpi", "overall"]):
        return ["overall_performance"]
    if any(token in raw for token in ["time", "trend", "daily"]):
        return ["time_trend"]
    if any(token in raw for token in ["region", "regional", "geo"]):
        return ["regional_comparison"]
    if any(token in raw for token in ["product", "mix", "category"]):
        return ["product_mix"]
    return ["misc"]


def _build_artifacts_from_evidence_pack(evidence_pack: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts: List[Dict[str, Any]] = []

    for table in evidence_pack.get("tables", []) or []:
        if not isinstance(table, dict):
            continue
        artifacts.append(
            {
                "artifact_id": table.get("table_id", ""),
                "artifact_type": "table",
                "title": table.get("title", table.get("table_id", "未命名表格")),
                "path": table.get("path", ""),
                "format": table.get("format", "csv"),
                "topic_tags": _infer_table_topic_tags(table),
            }
        )

    for chart in evidence_pack.get("charts", []) or []:
        if not isinstance(chart, dict):
            continue
        artifacts.append(
            {
                "artifact_id": chart.get("chart_id", ""),
                "artifact_type": "chart",
                "title": chart.get("title", chart.get("chart_id", "未命名图表")),
                "path": chart.get("path", ""),
                "format": Path(str(chart.get("path", ""))).suffix.lstrip(".").lower() or "png",
                "topic_tags": chart.get("topic_tags", []) or ["misc"],
            }
        )

    return artifacts


def _build_findings_from_evidence_pack(evidence_pack: Dict[str, Any]) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for finding in evidence_pack.get("findings", []) or []:
        if not isinstance(finding, dict):
            continue
        findings.append(
            {
                "finding_id": finding.get("finding_id", ""),
                "title": finding.get("title", ""),
                "statement": finding.get("statement", ""),
                "description": finding.get("statement", ""),
                "category": finding.get("category", ""),
                "importance": finding.get("importance", ""),
                "confidence": finding.get("confidence", ""),
                "topic_tags": finding.get("topic_tags", []) or [],
            }
        )
    return findings


def _build_claims_from_evidence_pack(evidence_pack: Dict[str, Any]) -> List[Dict[str, Any]]:
    finding_topic_map = {
        str(item.get("finding_id", "")): list(item.get("topic_tags", []) or [])
        for item in evidence_pack.get("findings", []) or []
        if isinstance(item, dict)
    }

    claims: List[Dict[str, Any]] = []
    for claim in evidence_pack.get("claim_evidence_map", []) or []:
        if not isinstance(claim, dict):
            continue

        support = claim.get("support", {}) or {}
        topic_tags: List[str] = []
        for finding_id in support.get("finding_ids", []) or []:
            for tag in finding_topic_map.get(str(finding_id), []):
                if tag not in topic_tags:
                    topic_tags.append(tag)

        claim_text = str(claim.get("claim_text", "")).strip()
        claims.append(
            {
                "claim_id": claim.get("claim_id", ""),
                "claim_text": claim_text,
                "claim": claim_text,
                "confidence": claim.get("confidence", ""),
                "topic_tags": topic_tags,
            }
        )

    return claims


def _build_caveats_from_evidence_pack(evidence_pack: Dict[str, Any]) -> List[Dict[str, Any]]:
    caveats: List[Dict[str, Any]] = []
    for caveat in evidence_pack.get("caveats", []) or []:
        if not isinstance(caveat, dict):
            continue
        caveats.append(
            {
                "caveat_id": caveat.get("caveat_id", ""),
                "message": caveat.get("message", ""),
                "severity": caveat.get("severity", "medium"),
            }
        )
    return caveats


def _build_report_payload_from_state(state: AnalysisGraphState) -> Dict[str, Any]:
    evidence_pack = state.get("evidence_pack", {}) or {}
    workspace = state.get("analysis_workspace", {}) or {}

    return {
        "request_id": state.get("request_id", "unknown_request"),
        "dataset_id": state.get("dataset_id", ""),
        "dataset_path": state.get("dataset_path", ""),
        "user_prompt": state.get("user_prompt", ""),
        "normalized_task": state.get("normalized_task", {}) or {},
        "dataset_context": state.get("dataset_context", {}) or {},
        "analysis_brief": state.get("analysis_brief", {}) or {},
        "findings": _build_findings_from_evidence_pack(evidence_pack),
        "claims": _build_claims_from_evidence_pack(evidence_pack),
        "caveats": _build_caveats_from_evidence_pack(evidence_pack),
        "artifacts": _build_artifacts_from_evidence_pack(evidence_pack),
        "run_metadata": {
            "workspace_root": workspace.get("root_dir", ""),
            "dataset_path": state.get("dataset_path", ""),
        },
    }


def _resolve_report_output_path(state: AnalysisGraphState) -> Path:
    workspace = state.get("analysis_workspace", {}) or {}
    outputs_dir = str(workspace.get("outputs_dir", "")).strip()
    if outputs_dir:
        return Path(outputs_dir).resolve() / "report.md"

    fallback_dir = Path("app/artifacts/report_drafts")
    fallback_dir.mkdir(parents=True, exist_ok=True)
    return (fallback_dir / f"{state.get('request_id', 'unknown_request')}_report.md").resolve()


def _resolve_json_path(state: AnalysisGraphState, report_path: Path) -> Path:
    workspace = state.get("analysis_workspace", {}) or {}
    outputs_dir = str(workspace.get("outputs_dir", "")).strip()
    if outputs_dir:
        candidate = Path(outputs_dir) / "structured_result.json"
        if candidate.exists():
            return candidate.resolve()
    return report_path


def _extract_title_from_content(content: str) -> str:
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return stripped[2:].strip()
    return "数据分析报告"


def write_report_node(state: AnalysisGraphState) -> AnalysisGraphState:
    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    analysis_brief = state.get("analysis_brief")
    evidence_pack = state.get("evidence_pack")
    review_result = state.get("review_result", {}) or {}
    validation_result = state.get("validation_result", {}) or {}
    degraded_output = state.get("degraded_output", False)

    if not analysis_brief or analysis_brief.get("status") == "stub":
        errors.append(
            {
                "type": "missing_analysis_brief",
                "message": "analysis_brief is required before write_report.",
            }
        )
        return cast(AnalysisGraphState, {**state, "status": "FAILED", "errors": errors, "warnings": warnings})

    if not evidence_pack or evidence_pack.get("status") == "stub":
        errors.append(
            {
                "type": "missing_evidence_pack",
                "message": "evidence_pack is required before write_report.",
            }
        )
        return cast(AnalysisGraphState, {**state, "status": "FAILED", "errors": errors, "warnings": warnings})

    try:
        payload = _build_report_payload_from_state(state)
        report_path = _resolve_report_output_path(state)
        json_path = _resolve_json_path(state, report_path)
        generated_path = generate_report_stream_to_file(
            payload,
            report_path,
            json_path=json_path,
            echo=False,
        ).resolve()

        content = generated_path.read_text(encoding="utf-8")
        title = _extract_title_from_content(content)
        subtitle = f"面向 {analysis_brief.get('target_audience', 'unknown')} 的数据分析输出"

        used_chart_ids = [c.get("chart_id") for c in evidence_pack.get("charts", []) if c.get("chart_id")]
        used_table_ids = [t.get("table_id") for t in evidence_pack.get("tables", []) if t.get("table_id")]
        used_finding_ids = [f.get("finding_id") for f in evidence_pack.get("findings", []) if f.get("finding_id")]

        report_draft = ReportDraft(
            title=title,
            subtitle=subtitle,
            content=content,
            degraded_output=degraded_output,
            used_chart_ids=used_chart_ids,
            used_table_ids=used_table_ids,
            used_finding_ids=used_finding_ids,
            report_metadata={
                "generator": "write_report_node",
                "report_path": str(generated_path),
                "review": {
                    "approved": review_result.get("approved", False),
                    "score": review_result.get("score", 0.0),
                    "severity": review_result.get("severity", "unknown"),
                    "review_id": review_result.get("review_id"),
                },
                "validation": {
                    "valid": validation_result.get("valid", False),
                    "hard_error_count": len(validation_result.get("hard_errors", []) or []),
                    "warning_count": len(validation_result.get("warnings", []) or []),
                },
                "execution_mode": state.get("execution_mode", "normal"),
            },
        )

        return cast(
            AnalysisGraphState,
            {
                **state,
                "report_draft": report_draft.model_dump(),
                "status": "REPORT_WRITTEN",
                "warnings": warnings,
                "errors": errors,
            },
        )

    except Exception as exc:
        errors.append(
            {
                "type": "write_report_failed",
                "message": str(exc),
            }
        )
        return cast(AnalysisGraphState, {**state, "status": "FAILED", "errors": errors, "warnings": warnings})
