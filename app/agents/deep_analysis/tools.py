from __future__ import annotations
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from langchain_core.tools import tool
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.services.dataframe_io import load_dataframe


def _ensure_parent(path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _to_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _infer_grain_from_times(series: pd.Series) -> str:
    s = _safe_to_datetime(series).dropna().sort_values()
    if len(s) < 2:
        return "day"

    diffs = s.diff().dropna()
    if diffs.empty:
        return "day"

    median_days = diffs.dt.total_seconds().median() / 86400.0
    if median_days <= 2:
        return "day"
    if median_days <= 10:
        return "week"
    if median_days <= 45:
        return "month"
    if median_days <= 120:
        return "quarter"
    return "year"


def _periodize(series: pd.Series, grain: str) -> pd.Series:
    dt = _safe_to_datetime(series)
    if grain == "day":
        return dt.dt.strftime("%Y-%m-%d")
    if grain == "week":
        return dt.dt.to_period("W").astype(str)
    if grain == "month":
        return dt.dt.to_period("M").astype(str)
    if grain == "quarter":
        return dt.dt.to_period("Q").astype(str)
    if grain == "year":
        return dt.dt.to_period("Y").astype(str)
    return dt.dt.strftime("%Y-%m-%d")


@tool
def inspect_dataset_tool(dataset_path: str, max_rows: int = 5) -> str:
    """
    Inspect dataset shape, columns, dtypes, and preview rows.
    """
    df = load_dataframe(dataset_path)
    payload: Dict[str, Any] = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "preview": df.head(max_rows).to_dict(orient="records"),
    }
    return _to_json(payload)


@tool
def profile_columns_tool(dataset_path: str, max_sample_values: int = 5) -> str:
    """
    Build a lightweight profile for all columns in the dataset.
    """
    df = load_dataframe(dataset_path)

    rows: List[Dict[str, Any]] = []
    for col in df.columns:
        s = df[col]
        rows.append(
            {
                "name": col,
                "dtype": str(s.dtype),
                "null_ratio": round(float(s.isna().mean()), 4),
                "non_null_count": int(s.notna().sum()),
                "unique_count": int(s.nunique(dropna=True)),
                "sample_values": s.dropna().astype(str).unique().tolist()[:max_sample_values],
            }
        )

    return _to_json(
        {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns_profile": rows,
        }
    )

def _ensure_parent(path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _to_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


@tool
def summarize_metrics_tool(
    dataset_path: str,
    metrics: List[str],
    output_csv_path: str,
) -> str:
    """
    Build a summary KPI table for the given metrics and save it to CSV.
    """
    df = load_dataframe(dataset_path)

    valid_metrics = [m for m in metrics if m in df.columns]
    if not valid_metrics:
        raise ValueError("No valid metrics were provided.")

    rows = []
    for metric in valid_metrics:
        s = pd.to_numeric(df[metric], errors="coerce")
        rows.append(
            {
                "metric": metric,
                "sum": float(s.sum()),
                "mean": float(s.mean()) if s.notna().any() else None,
                "median": float(s.median()) if s.notna().any() else None,
                "non_null_count": int(s.notna().sum()),
            }
        )

    out_df = pd.DataFrame(rows)
    out_path = _ensure_parent(output_csv_path)
    out_df.to_csv(out_path, index=False)

    return _to_json(
        {
            "table_path": str(out_path),
            "row_count": len(out_df),
            "columns": list(out_df.columns),
            "topic_tags": ["overall_performance"],
        }
    )


@tool
def time_trend_tool(
    dataset_path: str,
    time_col: str,
    metrics: List[str],
    output_csv_path: str,
    output_chart_path: str,
    grain: Optional[str] = None,
) -> str:
    """
    Aggregate metrics over time, save trend table to CSV and line chart to PNG.
    """
    df = load_dataframe(dataset_path)
    if time_col not in df.columns:
        raise ValueError(f"time_col not found: {time_col}")

    valid_metrics = [m for m in metrics if m in df.columns]
    if not valid_metrics:
        raise ValueError("No valid metrics were provided.")

    tmp = df.copy()
    tmp[time_col] = _safe_to_datetime(tmp[time_col])
    tmp = tmp.dropna(subset=[time_col])
    if tmp.empty:
        raise ValueError("No valid datetime rows after parsing time column.")

    selected_grain = grain or _infer_grain_from_times(tmp[time_col])
    tmp["_period"] = _periodize(tmp[time_col], selected_grain)

    agg_map = {m: "sum" for m in valid_metrics}
    trend = tmp.groupby("_period", as_index=False).agg(agg_map)
    trend = trend.rename(columns={"_period": time_col})

    csv_path = _ensure_parent(output_csv_path)
    chart_path = _ensure_parent(output_chart_path)

    trend.to_csv(csv_path, index=False)

    plt.figure(figsize=(8, 4.5))
    for metric in valid_metrics[:3]:
        plt.plot(trend[time_col], trend[metric], marker="o", label=metric)
    plt.title(f"Time Trend ({selected_grain})")
    plt.xlabel(time_col)
    plt.ylabel(", ".join(valid_metrics[:3]))
    if len(valid_metrics[:3]) > 1:
        plt.legend()
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close()

    return _to_json(
        {
            "table_path": str(csv_path),
            "chart_path": str(chart_path),
            "time_col": time_col,
            "metrics": valid_metrics,
            "grain": selected_grain,
            "row_count": len(trend),
            "topic_tags": ["time_trend"],
        }
    )


@tool
def group_compare_tool(
    dataset_path: str,
    group_col: str,
    metrics: List[str],
    output_csv_path: str,
) -> str:
    """
    Group by one dimension and aggregate metrics by sum, then save to CSV.
    """
    df = load_dataframe(dataset_path)

    if group_col not in df.columns:
        raise ValueError(f"group_col not found: {group_col}")

    valid_metrics = [m for m in metrics if m in df.columns]
    if not valid_metrics:
        raise ValueError("No valid metrics were provided.")

    agg_map = {m: "sum" for m in valid_metrics}
    grouped = df.groupby(group_col, as_index=False).agg(agg_map)

    primary = valid_metrics[0]
    grouped = grouped.sort_values(primary, ascending=False)

    out_path = _ensure_parent(output_csv_path)
    grouped.to_csv(out_path, index=False)

    return _to_json(
        {
            "table_path": str(out_path),
            "row_count": len(grouped),
            "columns": list(grouped.columns),
            "group_col": group_col,
            "metrics": valid_metrics,
        }
    )


@tool
def group_compare_chart_tool(
    dataset_path: str,
    group_col: str,
    metrics: List[str],
    output_csv_path: str,
    output_chart_path: str,
    top_n: int = 15,
) -> str:
    """
    Build a grouped comparison table and a bar chart, then save both.
    """
    df = load_dataframe(dataset_path)

    if group_col not in df.columns:
        raise ValueError(f"group_col not found: {group_col}")

    valid_metrics = [m for m in metrics if m in df.columns]
    if not valid_metrics:
        raise ValueError("No valid metrics were provided.")

    agg_map = {m: "sum" for m in valid_metrics}
    grouped = df.groupby(group_col, as_index=False).agg(agg_map)

    primary = valid_metrics[0]
    grouped = grouped.sort_values(primary, ascending=False).head(top_n)

    csv_path = _ensure_parent(output_csv_path)
    chart_path = _ensure_parent(output_chart_path)

    grouped.to_csv(csv_path, index=False)

    plt.figure(figsize=(8, 4.5))
    plt.bar(grouped[group_col].astype(str), grouped[primary])
    plt.title(f"{group_col} Comparison")
    plt.xlabel(group_col)
    plt.ylabel(primary)
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close()

    topic_tags: List[str] = []
    low = group_col.lower()
    if "region" in low or "area" in low or "地区" in group_col or "区域" in group_col:
        topic_tags.append("regional_comparison")
    if "product" in low or "category" in low or "产品" in group_col or "品类" in group_col:
        topic_tags.append("product_mix")

    return _to_json(
        {
            "table_path": str(csv_path),
            "chart_path": str(chart_path),
            "row_count": len(grouped),
            "columns": list(grouped.columns),
            "group_col": group_col,
            "metrics": valid_metrics,
            "topic_tags": topic_tags,
        }
    )


@tool
def register_artifact_tool(
    artifact_id: str,
    artifact_type: str,
    title: str,
    path: str,
    topic_tags: Optional[List[str]] = None,
    description: Optional[str] = None,
    format: Optional[str] = None,
) -> str:
    """
    Register an already-created artifact into a normalized artifact payload.
    This does not create the file. The file must already exist.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Artifact file does not exist: {path}")

    inferred_format = format or p.suffix.lstrip(".").lower() or None

    return _to_json(
        {
            "artifact_id": artifact_id,
            "artifact_type": artifact_type,
            "title": title,
            "path": str(p),
            "format": inferred_format,
            "topic_tags": topic_tags or [],
            "description": description,
        }
    )


@tool
def finalize_structured_output_tool(
    output_path: str,
    payload_json: str,
) -> str:
    """
    Normalize top-level keys of a structured analysis payload and write it to JSON.
    """
    default_payload = {
        "plan": {},
        "planned_actions": [],
        "executed_steps": [],
        "artifacts": [],
        "findings": [],
        "claims": [],
        "caveats": [],
        "rejected_charts": [],
        "rejected_hypotheses": [],
        "trace": [],
        "run_metadata": {},
    }

    payload = json.loads(payload_json)
    if not isinstance(payload, dict):
        raise ValueError("Structured output payload must be a JSON object.")

    missing = sorted(set(default_payload) - set(payload))
    normalized_payload = {
        **default_payload,
        **payload,
    }

    out_path = _ensure_parent(output_path)
    out_path.write_text(
        json.dumps(normalized_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return _to_json(
        {
            "structured_output_path": str(out_path),
            "written": True,
            "missing_keys": missing,
        }
    )


def get_deep_analysis_tools():
    return [
        inspect_dataset_tool,
        profile_columns_tool,
        summarize_metrics_tool,
        time_trend_tool,
        group_compare_tool,
        group_compare_chart_tool,
        register_artifact_tool,
        finalize_structured_output_tool,
    ]
{'publish': {'analysis_brief': {'brief_id': 'brief_001',
                                'brief_notes': 'rule_based_plus_llm_refine',
                                'business_goal': '生成可发布的数据分析报告',
                                'chart_policy': {'avoid_chart_types': ['low_information_pie',
                                                                       'duplicate_histogram'],
                                                 'max_similar_chart_per_metric': 2,
                                                 'max_total_charts': 10,
                                                 'preferred_chart_types': ['line',
                                                                           'bar',
                                                                           'stacked_bar',
                                                                           'heatmap'],
                                                 'target_chart_range': [6, 10]},
                                'completion_criteria': ['所有 must_cover_topics '
                                                        '均被覆盖',
                                                        '每个核心结论可追溯到图表或表格证据',
                                                        '报告需包含执行摘要、主体分析、风险与建议',
                                                        '至少包含一张有效的时间趋势图',
                                                        '至少包含一组区域对比分析',
                                                        '至少包含一组产品结构分析'],
                                'confidence_policy': {'default_claim_level': 'descriptive_or_associational',
                                                      'forbid_causal_language_without_evidence': True},
                                'must_cover_topics': ['overall_performance',
                                                      'time_trend',
                                                      'regional_comparison',
                                                      'product_mix'],
                                'must_not_do': ['未经证据支持的因果推断',
                                                '生成重复信息量图表',
                                                '使用不在数据中的业务背景作强结论',
                                                '在 Writer 阶段新增未经 evidence_pack '
                                                '支持的事实'],
                                'optional_topics': [],
                                'recommended_dimensions': ['region',
                                                           'category',
                                                           'order_date'],
                                'recommended_metrics': ['gmv', 'cost'],
                                'report_style': {'detail_level': 'high',
                                                 'language': 'zh-CN',
                                                 'tone': 'professional'},
                                'revision_policy': {'max_review_rounds': 2,
                                                    'must_fix_first': True},
                                'table_policy': {'max_total_tables': 6,
                                                 'must_have_tables': ['summary_kpi_table',
                                                                      'regional_comparison_table']},
                                'target_audience': 'business_stakeholders',
                                'task_type': 'reporting',
                                'version': 1},
             'analysis_workspace': {'charts_dir': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\charts',
                                    'dataset_local_path': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\input\\demo_sales.csv',
                                    'input_dir': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\input',
                                    'logs_dir': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\logs',
                                    'outputs_dir': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\outputs',
                                    'root_dir': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2',
                                    'scripts_dir': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\scripts',
                                    'tables_dir': 'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\tables'},
             'dataset_context': {'business_hints': ['该数据集适合做时间趋势分析。',
                                                    '该数据集适合做区域对比分析。',
                                                    '该数据集适合做产品结构与品类分析。',
                                                    '该数据集适合做销售与利润表现分析。'],
                                 'candidate_dimension_columns': ['region',
                                                                 'category',
                                                                 'user_id'],
                                 'candidate_id_columns': ['order_id',
                                                          'user_id'],
                                 'candidate_measure_columns': ['gmv', 'cost'],
                                 'candidate_time_columns': ['order_date'],
                                 'data_quality_summary': {'duplicate_rows_ratio': 0.0,
                                                          'high_cardinality_columns': [],
                                                          'missingness': [{'column': 'gmv',
                                                                           'null_ratio': 0.1},
                                                                          {'column': 'cost',
                                                                           'null_ratio': 0.1}],
                                                          'potential_outliers': []},
                                 'dataset_id': 'ds_002',
                                 'profile_version': '1.0',
                                 'source_path': './data/demo_sales.csv',
                                 'tables': [{'column_count': 7,
                                             'columns': [{'name': 'order_id',
                                                          'non_null_count': 10,
                                                          'null_ratio': 0.0,
                                                          'physical_type': 'int',
                                                          'role_candidates': ['identifier'],
                                                          'sample_values': ['1',
                                                                            '2',
                                                                            '3',
                                                                            '4',
                                                                            '5'],
                                                          'semantic_confidence': 0.95,
                                                          'semantic_type': 'id',
                                                          'unique_count': 10,
                                                          'unique_ratio': 1.0},
                                                         {'name': 'order_date',
                                                          'non_null_count': 10,
                                                          'null_ratio': 0.0,
                                                          'physical_type': 'datetime',
                                                          'role_candidates': ['time_dimension'],
                                                          'sample_values': ['2026-03-01',
                                                                            '2026-03-02',
                                                                            '2026-03-03',
                                                                            '2026-03-04',
                                                                            '2026-03-05'],
                                                          'semantic_confidence': 0.9,
                                                          'semantic_type': 'date',
                                                          'unique_count': 5,
                                                          'unique_ratio': 0.5},
                                                         {'name': 'region',
                                                          'non_null_count': 10,
                                                          'null_ratio': 0.0,
                                                          'physical_type': 'string',
                                                          'role_candidates': ['business_dimension',
                                                                              'geo_dimension'],
                                                          'sample_values': ['East',
                                                                            'West',
                                                                            'South',
                                                                            'North'],
                                                          'semantic_confidence': 0.85,
                                                          'semantic_type': 'category',
                                                          'unique_count': 4,
                                                          'unique_ratio': 0.4},
                                                         {'name': 'category',
                                                          'non_null_count': 10,
                                                          'null_ratio': 0.0,
                                                          'physical_type': 'string',
                                                          'role_candidates': ['business_dimension',
                                                                              'product_dimension'],
                                                          'sample_values': ['Electronics',
                                                                            'Home',
                                                                            'Beauty'],
                                                          'semantic_confidence': 0.85,
                                                          'semantic_type': 'category',
                                                          'unique_count': 3,
                                                          'unique_ratio': 0.3},
                                                         {'name': 'gmv',
                                                          'non_null_count': 9,
                                                          'null_ratio': 0.1,
                                                          'physical_type': 'float',
                                                          'role_candidates': ['measure'],
                                                          'sample_values': ['1200.0',
                                                                            '500.0',
                                                                            '1800.0',
                                                                            '300.0',
                                                                            '700.0'],
                                                          'semantic_confidence': 0.9,
                                                          'semantic_type': 'metric',
                                                          'unique_count': 9,
                                                          'unique_ratio': 0.9},
                                                         {'name': 'cost',
                                                          'non_null_count': 9,
                                                          'null_ratio': 0.1,
                                                          'physical_type': 'float',
                                                          'role_candidates': ['measure'],
                                                          'sample_values': ['800.0',
                                                                            '300.0',
                                                                            '1200.0',
                                                                            '120.0',
                                                                            '420.0'],
                                                          'semantic_confidence': 0.9,
                                                          'semantic_type': 'metric',
                                                          'unique_count': 9,
                                                          'unique_ratio': 0.9},
                                                         {'name': 'user_id',
                                                          'non_null_count': 10,
                                                          'null_ratio': 0.0,
                                                          'physical_type': 'string',
                                                          'role_candidates': ['identifier',
                                                                              'customer_dimension'],
                                                          'sample_values': ['u1',
                                                                            'u2',
                                                                            'u3',
                                                                            'u4',
                                                                            'u5'],
                                                          'semantic_confidence': 0.95,
                                                          'semantic_type': 'id',
                                                          'unique_count': 10,
                                                          'unique_ratio': 1.0}],
                                             'row_count': 10,
                                             'table_name': 'demo_sales.csv'}],
                                 'time_coverage': {'granularity_candidates': ['day'],
                                                   'max': '2026-03-05',
                                                   'min': '2026-03-01'},
                                 'warnings': []},
             'dataset_id': 'ds_002',
             'dataset_path': './data/demo_sales.csv',
             'degraded_output': True,
             'errors': [{'message': 'Fallback analysis script failed. stderr '
                                    'log: '
                                    'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_0\\logs\\script_stderr.log',
                         'type': 'deep_analysis_failed'},
                        {'message': 'evidence_pack is required before '
                                    'validate_evidence.',
                         'type': 'missing_evidence_pack'},
                        {'message': 'evidence_pack is required before '
                                    'review_evidence.',
                         'type': 'missing_evidence_pack'},
                        {'message': 'Fallback analysis script failed. stderr '
                                    'log: '
                                    'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_1\\logs\\script_stderr.log',
                         'type': 'deep_analysis_failed'},
                        {'message': 'evidence_pack is required before '
                                    'validate_evidence.',
                         'type': 'missing_evidence_pack'},
                        {'message': 'evidence_pack is required before '
                                    'review_evidence.',
                         'type': 'missing_evidence_pack'},
                        {'message': 'Fallback analysis script failed. stderr '
                                    'log: '
                                    'E:\\myagent\\analysis_agent\\app\\artifacts\\deepagent_runs\\req_002\\round_2\\logs\\script_stderr.log',
                         'type': 'deep_analysis_failed'},
                        {'message': 'evidence_pack is required before '
                                    'validate_evidence.',
                         'type': 'missing_evidence_pack'},
                        {'message': 'evidence_pack is required before '
                                    'review_evidence.',
                         'type': 'missing_evidence_pack'},
                        {'message': 'evidence_pack is required before '
                                    'write_report.',
                         'type': 'missing_evidence_pack'},
                        {'message': 'report_draft is required before final_qa.',
                         'type': 'missing_report_draft'},
                        {'message': 'report_draft is required before publish.',
                         'type': 'missing_report_draft'}],
             'execution_mode': 'degraded',
             'input_config': {'language': 'zh-CN',
                              'output_format': ['markdown']},
             'max_review_rounds': 2,
             'memory_context': {},
             'normalized_task': {'ambiguities': [{'fallback_policy': 'infer_from_dataset',
                                                  'field': 'time_scope',
                                                  'status': 'unspecified'}],
                                 'analysis_mode': 'reporting',
                                 'business_goal': '生成可发布的数据分析报告',
                                 'constraints': {'desired_output_formats': ['markdown'],
                                                 'detail_level': 'high',
                                                 'language': 'zh-CN',
                                                 'prefer_visualization': True},
                                 'normalization_notes': 'rule_based_plus_llm_refine',
                                 'primary_questions': ['时间趋势',
                                                       '区域差异',
                                                       '产品结构',
                                                       '销售表现'],
                                 'success_intent': 'produce_publishable_analysis_report',
                                 'target_audience': 'business_stakeholders',
                                 'task_type': 'reporting'},
             'request_id': 'req_002',
             'revision_context': {'mode': 'targeted_patch',
                                  'must_fix': [],
                                  'nice_to_have': [],
                                  'revision_tasks': [],
                                  'round': 2,
                                  'should_fix': [],
                                  'source_review_id': None},
             'revision_round': 2,
             'revision_tasks': [],
             'session_id': 'sess_002',
             'status': 'FAILED',
             'user_id': 'user_002',
             'user_prompt': '请做一份详细且图表丰富的销售分析报告，重点关注区域、产品和时间趋势。',
             'warnings': []}}
