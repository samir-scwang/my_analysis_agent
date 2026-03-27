from __future__ import annotations

import json
import os
import re
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_object_dtype,
    is_string_dtype,
)

from app.schemas.dataset import (
    ColumnProfile,
    DataQualitySummary,
    DatasetContext,
    MissingnessItem,
    OutlierHint,
    TableProfile,
    TimeCoverage,
)
from app.schemas.state import AnalysisGraphState
from app.services.dataframe_io import load_dataframe
from app.services.llm_service import LLMService


_DATE_LIKE_PATTERN = re.compile(
    r"^\d{4}([-/]\d{1,2}([-/]\d{1,2})?)?"
    r"(|\s+\d{1,2}:\d{1,2}(:\d{1,2})?)$"
)

_NUMERIC_TEXT_PATTERN = re.compile(r"^[+-]?\d+(\.\d+)?$")


def json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


def _infer_physical_type(series: pd.Series) -> str:
    if is_bool_dtype(series):
        return "bool"
    if is_datetime64_any_dtype(series):
        return "datetime"
    if is_integer_dtype(series):
        return "int"
    if is_float_dtype(series):
        return "float"
    if is_string_dtype(series) or is_object_dtype(series):
        return "string"
    return "unknown"


def _safe_to_datetime(series: pd.Series) -> pd.Series:
    """
    安静地尝试解析 datetime，避免 pandas 对非日期文本反复报 warning。
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        try:
            return pd.to_datetime(series, errors="coerce", format="mixed")
        except TypeError:
            return pd.to_datetime(series, errors="coerce")
        except Exception:
            return pd.to_datetime(series, errors="coerce")


def _looks_like_datetime_text(series: pd.Series) -> bool:
    if not (is_object_dtype(series) or is_string_dtype(series)):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return False

    sample = non_null.astype(str).str.strip().head(80)
    if sample.empty:
        return False

    # 先做强规则过滤，避免对明显非日期文本频繁调用 to_datetime
    regex_ratio = float(sample.str.match(_DATE_LIKE_PATTERN, na=False).mean())

    # 只有当至少有一部分看起来像日期时，再去做真实解析
    if regex_ratio < 0.2:
        return False

    parsed = _safe_to_datetime(sample)
    parse_ratio = float(parsed.notna().mean())
    return max(regex_ratio, parse_ratio) >= 0.6


def _try_parse_datetime(series: pd.Series) -> bool:
    if not (is_object_dtype(series) or is_string_dtype(series)):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return False

    if not _looks_like_datetime_text(series):
        return False

    sample = non_null.astype(str).str.strip().head(100)
    parsed = _safe_to_datetime(sample)
    return float(parsed.notna().mean()) >= 0.8


def _try_parse_numeric_text(series: pd.Series) -> bool:
    if not (is_object_dtype(series) or is_string_dtype(series)):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return False

    sample = (
        non_null.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .head(100)
    )
    if sample.empty:
        return False

    regex_ratio = float(sample.str.match(_NUMERIC_TEXT_PATTERN, na=False).mean())
    if regex_ratio >= 0.8:
        return True

    try:
        parsed = pd.to_numeric(sample, errors="coerce")
        return float(parsed.notna().mean()) >= 0.8
    except Exception:
        return False


def _is_id_name(col_name: str) -> bool:
    name = col_name.lower().strip()

    direct_keywords = {
        "id",
        "uid",
        "uuid",
        "pk",
        "key",
        "code",
        "编号",
        "编码",
        "流水号",
        "单号",
        "订单号",
        "客户编号",
        "产品编码",
        "商品编码",
        "sku",
    }
    if name in direct_keywords:
        return True

    if name.endswith("_id") or name.startswith("id_"):
        return True

    tokens = [token for token in re.split(r"[_\W]+", name) if token]
    id_like_tokens = {
        "id",
        "uid",
        "uuid",
        "code",
        "no",
        "num",
        "number",
        "sn",
        "sku",
        "key",
    }
    return any(token in id_like_tokens for token in tokens)


def _infer_semantic_type_rule(
    col_name: str,
    series: pd.Series,
    precomputed_physical_type: Optional[str] = None,
    is_datetime_like: bool = False,
    is_numeric_text: bool = False,
) -> tuple[str, float]:
    name = col_name.lower()
    physical = precomputed_physical_type or _infer_physical_type(series)
    unique_count = int(series.nunique(dropna=True))
    unique_ratio = unique_count / max(len(series), 1)

    if _is_id_name(name) and unique_ratio >= 0.2:
        return "id", 0.90

    if "date" in name or "time" in name or "日期" in name or "时间" in name or is_datetime_like:
        return "date", 0.88

    if physical in ["int", "float"] or is_numeric_text:
        time_like_numeric_names = {
            "year",
            "month",
            "day",
            "week",
            "quarter",
            "年份",
            "月份",
            "周",
            "季度",
            "日期",
        }
        if any(tok in name for tok in time_like_numeric_names):
            return "category", 0.70

        id_like_name = _is_id_name(name)
        if id_like_name and unique_ratio >= 0.2:
            return "id", 0.85

        if unique_count <= 20 and unique_ratio <= 0.05:
            return "category", 0.75

        return "metric", 0.88

    if physical == "string":
        if _is_id_name(name) and unique_ratio >= 0.2:
            return "id", 0.86

        if unique_ratio > 0.85:
            return "text", 0.62

        return "category", 0.82

    return "unknown", 0.50


def _infer_role_candidates_rule(
    col_name: str,
    physical_type: str,
    semantic_type: str,
    unique_ratio: float,
) -> List[str]:
    roles: List[str] = []
    name = col_name.lower()

    if semantic_type == "date":
        roles.append("time_dimension")

    if semantic_type == "metric":
        roles.append("measure")

    if semantic_type == "category":
        roles.append("business_dimension")

    if semantic_type == "id":
        roles.append("identifier")

    geo_keywords = [
        "region", "area", "province", "city", "state", "country",
        "区域", "地区", "省", "市"
    ]
    product_keywords = [
        "product", "category", "sku", "item", "goods",
        "品类", "产品", "商品"
    ]
    customer_keywords = [
        "customer", "client", "user", "member", "account",
        "客户", "用户", "会员"
    ]

    if any(k in name for k in geo_keywords):
        roles.append("geo_dimension")

    if any(k in name for k in product_keywords):
        roles.append("product_dimension")

    # 对 id 列，如果名字明显是 customer/user，也允许带 customer_dimension 标签
    # 但后续 candidate_dimension_columns 会把 identifier 排除掉
    if any(k in name for k in customer_keywords):
        roles.append("customer_dimension")

    if any(
        k in name
        for k in [
            "sales", "revenue", "profit", "gmv", "amount", "price",
            "销量", "销售额", "收入", "利润", "金额"
        ]
    ):
        if "measure" not in roles and physical_type in ["int", "float"]:
            roles.append("measure")

    if unique_ratio > 0.95 and semantic_type in ["category", "text"]:
        roles.append("high_cardinality_dimension")

    return list(dict.fromkeys(roles))


def _safe_sample_values(series: pd.Series, max_n: int = 5) -> List[str]:
    values = series.dropna().astype(str).unique().tolist()
    return values[:max_n]


def _top_values(series: pd.Series, max_n: int = 5) -> List[str]:
    non_null = series.dropna()
    if non_null.empty:
        return []

    try:
        counts = non_null.astype(str).value_counts(dropna=True).head(max_n)
        return [f"{idx} ({int(cnt)})" for idx, cnt in counts.items()]
    except Exception:
        return _safe_sample_values(series, max_n=max_n)


def _numeric_summary(series: pd.Series) -> Dict[str, Any]:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return {}

    try:
        return {
            "min": round(float(numeric.min()), 4),
            "p25": round(float(numeric.quantile(0.25)), 4),
            "p50": round(float(numeric.quantile(0.50)), 4),
            "p75": round(float(numeric.quantile(0.75)), 4),
            "max": round(float(numeric.max()), 4),
            "mean": round(float(numeric.mean()), 4),
        }
    except Exception:
        return {}


def _datetime_summary(series: pd.Series) -> Dict[str, Any]:
    parsed = _safe_to_datetime(series).dropna()
    if parsed.empty:
        return {}

    try:
        return {
            "min": str(parsed.min()),
            "max": str(parsed.max()),
            "distinct_days": int(parsed.dt.date.nunique()),
        }
    except Exception:
        return {}


def _profile_column(df: pd.DataFrame, col_name: str) -> ColumnProfile:
    series = df[col_name]
    row_count = len(df)

    base_physical_type = _infer_physical_type(series)
    is_datetime_like = (
        base_physical_type == "datetime"
        or (base_physical_type == "string" and _try_parse_datetime(series))
    )
    is_numeric_text = (
        base_physical_type == "string"
        and not is_datetime_like
        and _try_parse_numeric_text(series)
    )

    if is_datetime_like:
        physical_type = "datetime"
    elif is_numeric_text:
        parsed = pd.to_numeric(
            series.astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )
        if parsed.notna().mean() >= 0.8:
            if (parsed.dropna() % 1 == 0).all():
                physical_type = "int"
            else:
                physical_type = "float"
        else:
            physical_type = base_physical_type
    else:
        physical_type = base_physical_type

    semantic_type, confidence = _infer_semantic_type_rule(
        col_name=col_name,
        series=series,
        precomputed_physical_type=physical_type,
        is_datetime_like=is_datetime_like,
        is_numeric_text=is_numeric_text,
    )

    null_ratio = float(series.isna().mean()) if row_count > 0 else 0.0
    unique_count = int(series.nunique(dropna=True))
    unique_ratio = float(unique_count / max(row_count, 1))
    non_null_count = int(series.notna().sum())

    roles = _infer_role_candidates_rule(
        col_name=col_name,
        physical_type=physical_type,
        semantic_type=semantic_type,
        unique_ratio=unique_ratio,
    )

    return ColumnProfile(
        name=col_name,
        physical_type=cast(Any, physical_type),
        semantic_type=cast(Any, semantic_type),
        null_ratio=round(null_ratio, 4),
        unique_ratio=round(unique_ratio, 4),
        non_null_count=non_null_count,
        unique_count=unique_count,
        sample_values=_safe_sample_values(series),
        role_candidates=roles,
        semantic_confidence=round(confidence, 2),
    )


def _detect_missingness(column_profiles: List[ColumnProfile]) -> List[MissingnessItem]:
    return [
        MissingnessItem(column=col.name, null_ratio=col.null_ratio)
        for col in column_profiles
        if col.null_ratio > 0
    ]


def _detect_high_cardinality(column_profiles: List[ColumnProfile], row_count: int) -> List[str]:
    cols: List[str] = []
    for col in column_profiles:
        # 纯 id 列不再混进 high_cardinality_columns，避免提示噪音
        if col.semantic_type == "id":
            continue

        if col.semantic_type not in ["category", "text"]:
            continue

        if col.unique_ratio >= 0.95:
            cols.append(col.name)
            continue

        if row_count >= 1000 and col.unique_count >= 100 and col.unique_ratio > 0.2:
            cols.append(col.name)
            continue

        if row_count < 1000 and col.unique_count >= 50 and col.unique_ratio > 0.3:
            cols.append(col.name)

    return cols


def _detect_outliers(df: pd.DataFrame, column_profiles: List[ColumnProfile]) -> List[OutlierHint]:
    hints: List[OutlierHint] = []

    for col in column_profiles:
        if col.physical_type not in ["int", "float"]:
            continue

        series = pd.to_numeric(df[col.name], errors="coerce").dropna()
        if len(series) < 20:
            continue

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            continue

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outlier_ratio = float(((series < lower) | (series > upper)).mean())

        if outlier_ratio > 0.01:
            hints.append(
                OutlierHint(
                    column=col.name,
                    method="iqr",
                    outlier_ratio=round(outlier_ratio, 4),
                )
            )

    return hints


def _duplicate_rows_ratio(df: pd.DataFrame) -> float:
    if len(df) == 0:
        return 0.0
    return round(float(df.duplicated().mean()), 4)


def _infer_time_coverage(df: pd.DataFrame, candidate_time_columns: List[str]) -> TimeCoverage:
    if not candidate_time_columns:
        return TimeCoverage()

    best_col: Optional[str] = None
    best_score = -1.0

    for col in candidate_time_columns:
        try:
            parsed = _safe_to_datetime(df[col])
            score = float(parsed.notna().mean())
            if score > best_score:
                best_score = score
                best_col = col
        except Exception:
            continue

    if not best_col:
        return TimeCoverage()

    series = _safe_to_datetime(df[best_col]).dropna()
    if series.empty:
        return TimeCoverage()

    min_dt = series.min()
    max_dt = series.max()
    span_days = (max_dt - min_dt).days if pd.notna(min_dt) and pd.notna(max_dt) else 0

    granularities: List[str] = []
    if span_days >= 1:
        granularities.append("day")
    if span_days >= 28:
        granularities.append("month")
    if span_days >= 90:
        granularities.append("quarter")
    if span_days >= 365:
        granularities.append("year")

    return TimeCoverage(
        min=str(min_dt.date()),
        max=str(max_dt.date()),
        granularity_candidates=granularities,
    )


def _generate_business_hints_rule(
    candidate_time_columns: List[str],
    candidate_measure_columns: List[str],
    candidate_dimension_columns: List[str],
) -> List[str]:
    hints: List[str] = []

    if candidate_time_columns and candidate_measure_columns:
        hints.append("该数据集适合做时间趋势分析。")

    lower_dims = [c.lower() for c in candidate_dimension_columns]
    lower_measures = [c.lower() for c in candidate_measure_columns]

    if any("region" in c or "area" in c or "区域" in c or "地区" in c for c in lower_dims):
        hints.append("该数据集适合做区域对比分析。")

    if any(
        "product" in c or "category" in c or "sku" in c or "品类" in c or "产品" in c
        for c in lower_dims
    ):
        hints.append("该数据集适合做产品结构与品类分析。")

    sales_metric_keywords = ["revenue", "sales", "profit", "gmv", "amount", "销售", "收入", "利润", "金额"]
    if any(any(k in m for k in sales_metric_keywords) for m in lower_measures):
        hints.append("该数据集适合做销售与利润表现分析。")

    if not hints and candidate_measure_columns:
        hints.append("该数据集适合做基础数值指标分析。")

    return hints


def _build_llm_prompt_payload(
    state: AnalysisGraphState,
    df: pd.DataFrame,
    column_profiles: List[ColumnProfile],
    quality_summary: DataQualitySummary,
    time_coverage: TimeCoverage,
) -> Dict[str, Any]:
    column_details: List[Dict[str, Any]] = []

    for col in column_profiles:
        series = df[col.name]
        item: Dict[str, Any] = {
            "name": col.name,
            "physical_type": col.physical_type,
            "rule_semantic_type": col.semantic_type,
            "rule_role_candidates": col.role_candidates,
            "null_ratio": col.null_ratio,
            "unique_ratio": col.unique_ratio,
            "non_null_count": col.non_null_count,
            "unique_count": col.unique_count,
            "sample_values": col.sample_values[:5],
            "top_values": _top_values(series, max_n=5),
        }

        if col.physical_type in ["int", "float"]:
            item["numeric_summary"] = _numeric_summary(series)
        elif col.physical_type == "datetime":
            item["datetime_summary"] = _datetime_summary(series)
        else:
            if _try_parse_datetime(series):
                item["datetime_summary"] = _datetime_summary(series)

        column_details.append(item)

    return {
        "user_prompt": state.get("user_prompt", ""),
        "normalized_task": state.get("normalized_task", {}),
        "dataset_overview": {
            "dataset_id": state.get("dataset_id", "unknown_dataset"),
            "source_path": state.get("dataset_path", ""),
            "row_count": len(df),
            "column_count": len(df.columns),
        },
        "columns": column_details,
        "quality_summary": quality_summary.model_dump(),
        "time_coverage_rule": time_coverage.model_dump(),
    }


def _safe_list_str(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []
    return [str(x) for x in value if str(x).strip()]


def _safe_float(value: Any, default: float = 0.5) -> float:
    try:
        v = float(value)
        return max(0.0, min(1.0, v))
    except Exception:
        return default


def _apply_llm_semantic_enrichment(
    state: AnalysisGraphState,
    df: pd.DataFrame,
    column_profiles: List[ColumnProfile],
    quality_summary: DataQualitySummary,
    time_coverage: TimeCoverage,
    warnings_list: List[Dict[str, Any]],
) -> tuple[
    List[ColumnProfile],
    List[str],
    List[str],
    List[str],
    List[str],
    List[str],
    List[Dict[str, Any]],
]:
    payload = _build_llm_prompt_payload(
        state=state,
        df=df,
        column_profiles=column_profiles,
        quality_summary=quality_summary,
        time_coverage=time_coverage,
    )

    system_prompt = """
你是一个资深数据分析建模助手，负责根据数据集列画像，做“业务语义识别”。

你必须遵守以下规则：
1. 你的任务不是重新做统计计算，而是基于给定画像做语义判断。
2. 你要尽量利用列名、样本值、top_values、数值摘要、时间摘要、用户任务。
3. 你必须输出严格 JSON。
4. semantic_type 只能取：
   ["metric", "category", "date", "id", "text", "unknown"]
5. role_candidates 只能从以下集合中选：
   ["time_dimension", "measure", "business_dimension", "identifier",
    "geo_dimension", "product_dimension", "customer_dimension",
    "high_cardinality_dimension"]
6. measure 一般应对应数值列；date 一般应对应可解析时间列；id 一般具有较高唯一性。
7. candidate_dimension_columns 应主要包含适合业务分析分组的维度，不要把纯 ID 列放进去。
8. business_hints 用中文输出，简洁、面向分析，不超过 6 条。
9. 如果规则结果明显合理，可以沿用；不要为修改而修改。
10. 输出格式必须是一个 JSON 对象，字段如下：
{
  "columns": [
    {
      "name": "...",
      "semantic_type": "...",
      "role_candidates": ["..."],
      "semantic_confidence": 0.0
    }
  ],
  "candidate_time_columns": ["..."],
  "candidate_measure_columns": ["..."],
  "candidate_dimension_columns": ["..."],
  "candidate_id_columns": ["..."],
  "business_hints": ["..."]
}
""".strip()

    user_prompt = (
        "请根据以下数据集画像结果做业务语义增强，并返回 JSON。\n\n"
        + json_dumps(payload)
    )

    try:
        llm = LLMService()
        result = llm.json_invoke(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            temperature=0.1,
        )

        by_name = {c.name: c for c in column_profiles}
        allowed_semantic = {"metric", "category", "date", "id", "text", "unknown"}
        allowed_roles = {
            "time_dimension",
            "measure",
            "business_dimension",
            "identifier",
            "geo_dimension",
            "product_dimension",
            "customer_dimension",
            "high_cardinality_dimension",
        }

        for item in result.get("columns", []):
            if not isinstance(item, dict):
                continue

            name = str(item.get("name", "")).strip()
            if not name or name not in by_name:
                continue

            col = by_name[name]
            semantic_type = str(item.get("semantic_type", col.semantic_type)).strip()
            if semantic_type not in allowed_semantic:
                semantic_type = col.semantic_type

            role_candidates = [
                str(x) for x in item.get("role_candidates", [])
                if str(x) in allowed_roles
            ]
            if not role_candidates:
                role_candidates = col.role_candidates

            conf = _safe_float(
                item.get("semantic_confidence", col.semantic_confidence),
                col.semantic_confidence,
            )

            if semantic_type == "measure" and col.physical_type not in ["int", "float"]:
                semantic_type = col.semantic_type

            if semantic_type == "date" and col.physical_type != "datetime":
                series = df[col.name]
                if not _try_parse_datetime(series):
                    semantic_type = col.semantic_type

            if semantic_type == "id" and col.unique_ratio < 0.05 and not _is_id_name(col.name):
                semantic_type = col.semantic_type

            if semantic_type == "metric" and col.physical_type not in ["int", "float"]:
                semantic_type = col.semantic_type

            if semantic_type == "id" and "identifier" not in role_candidates:
                role_candidates = list(dict.fromkeys(role_candidates + ["identifier"]))

            if semantic_type == "metric" and "measure" not in role_candidates and col.physical_type in ["int", "float"]:
                role_candidates = list(dict.fromkeys(role_candidates + ["measure"]))

            if semantic_type == "date" and "time_dimension" not in role_candidates:
                role_candidates = list(dict.fromkeys(role_candidates + ["time_dimension"]))

            if semantic_type == "category" and "business_dimension" not in role_candidates:
                role_candidates = list(dict.fromkeys(role_candidates + ["business_dimension"]))

            col.semantic_type = cast(Any, semantic_type)
            col.role_candidates = role_candidates
            col.semantic_confidence = round(conf, 2)

        all_names = set(by_name.keys())

        def _validated_names(names: List[str]) -> List[str]:
            return [n for n in names if n in all_names]

        candidate_time_columns = _validated_names(_safe_list_str(result.get("candidate_time_columns")))
        candidate_measure_columns = _validated_names(_safe_list_str(result.get("candidate_measure_columns")))
        candidate_dimension_columns = _validated_names(_safe_list_str(result.get("candidate_dimension_columns")))
        candidate_id_columns = _validated_names(_safe_list_str(result.get("candidate_id_columns")))
        business_hints = _safe_list_str(result.get("business_hints"))

        if not candidate_time_columns:
            candidate_time_columns = [
                c.name for c in column_profiles
                if c.semantic_type == "date" or "time_dimension" in c.role_candidates
            ]

        if not candidate_measure_columns:
            candidate_measure_columns = [
                c.name for c in column_profiles
                if "measure" in c.role_candidates and c.physical_type in ["int", "float"]
            ]

        if not candidate_dimension_columns:
            candidate_dimension_columns = [
                c.name for c in column_profiles
                if (
                    "business_dimension" in c.role_candidates
                    or "geo_dimension" in c.role_candidates
                    or "product_dimension" in c.role_candidates
                )
                and "identifier" not in c.role_candidates
            ]

        if not candidate_id_columns:
            candidate_id_columns = [
                c.name for c in column_profiles
                if "identifier" in c.role_candidates
            ]

        if not business_hints:
            business_hints = _generate_business_hints_rule(
                candidate_time_columns=candidate_time_columns,
                candidate_measure_columns=candidate_measure_columns,
                candidate_dimension_columns=candidate_dimension_columns,
            )

        # 再做一层强约束，避免 LLM 把 identifier 放进 dimension
        candidate_dimension_columns = [
            name
            for name in candidate_dimension_columns
            if "identifier" not in by_name[name].role_candidates
        ]

        return (
            column_profiles,
            candidate_time_columns,
            candidate_measure_columns,
            candidate_dimension_columns,
            candidate_id_columns,
            business_hints,
            warnings_list,
        )

    except Exception as e:
        warnings_list.append(
            {
                "type": "llm_semantic_enrichment_failed",
                "message": f"LLM semantic enrichment failed, fallback to rule-based result: {e}",
            }
        )

        candidate_time_columns = [
            c.name
            for c in column_profiles
            if c.semantic_type == "date" or "time_dimension" in c.role_candidates
        ]
        candidate_measure_columns = [
            c.name
            for c in column_profiles
            if "measure" in c.role_candidates
        ]
        candidate_dimension_columns = [
            c.name
            for c in column_profiles
            if (
                "business_dimension" in c.role_candidates
                or "geo_dimension" in c.role_candidates
                or "product_dimension" in c.role_candidates
            )
            and "identifier" not in c.role_candidates
        ]
        candidate_id_columns = [
            c.name
            for c in column_profiles
            if "identifier" in c.role_candidates
        ]
        business_hints = _generate_business_hints_rule(
            candidate_time_columns=candidate_time_columns,
            candidate_measure_columns=candidate_measure_columns,
            candidate_dimension_columns=candidate_dimension_columns,
        )

        return (
            column_profiles,
            candidate_time_columns,
            candidate_measure_columns,
            candidate_dimension_columns,
            candidate_id_columns,
            business_hints,
            warnings_list,
        )


def build_dataset_context_node(state: AnalysisGraphState) -> AnalysisGraphState:
    dataset_id = state.get("dataset_id", "unknown_dataset")
    dataset_path = state.get("dataset_path")

    warnings_list = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    if not dataset_path:
        errors.append(
            {
                "type": "missing_dataset_path",
                "message": "dataset_path is required for Dataset Context Builder.",
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "errors": errors,
                "warnings": warnings_list,
            },
        )

    try:
        df = load_dataframe(dataset_path)
        column_profiles = [_profile_column(df, col) for col in df.columns]

        table_profile = TableProfile(
            table_name=os.path.basename(dataset_path),
            row_count=len(df),
            column_count=len(df.columns),
            columns=column_profiles,
        )

        quality_summary = DataQualitySummary(
            missingness=_detect_missingness(column_profiles),
            high_cardinality_columns=_detect_high_cardinality(column_profiles, len(df)),
            potential_outliers=_detect_outliers(df, column_profiles),
            duplicate_rows_ratio=_duplicate_rows_ratio(df),
        )

        rule_candidate_time_columns = [
            c.name
            for c in column_profiles
            if c.semantic_type == "date" or "time_dimension" in c.role_candidates
        ]
        time_coverage = _infer_time_coverage(df, rule_candidate_time_columns)

        (
            column_profiles,
            candidate_time_columns,
            candidate_measure_columns,
            candidate_dimension_columns,
            candidate_id_columns,
            business_hints,
            warnings_list,
        ) = _apply_llm_semantic_enrichment(
            state=state,
            df=df,
            column_profiles=column_profiles,
            quality_summary=quality_summary,
            time_coverage=time_coverage,
            warnings_list=warnings_list,
        )

        if candidate_time_columns:
            time_coverage = _infer_time_coverage(df, candidate_time_columns)

        if len(df) == 0:
            warnings_list.append({"type": "empty_dataset", "message": "Dataset has zero rows."})

        if not candidate_measure_columns:
            warnings_list.append(
                {
                    "type": "no_measure_columns",
                    "message": "No obvious measure columns detected.",
                }
            )

        dataset_context = DatasetContext(
            dataset_id=dataset_id,
            source_path=dataset_path,
            tables=[
                TableProfile(
                    table_name=table_profile.table_name,
                    row_count=table_profile.row_count,
                    column_count=table_profile.column_count,
                    columns=column_profiles,
                )
            ],
            candidate_time_columns=candidate_time_columns,
            candidate_measure_columns=candidate_measure_columns,
            candidate_dimension_columns=candidate_dimension_columns,
            candidate_id_columns=candidate_id_columns,
            data_quality_summary=quality_summary,
            time_coverage=time_coverage,
            business_hints=business_hints,
            warnings=[
                w["message"] if isinstance(w, dict) and "message" in w else str(w)
                for w in warnings_list
            ],
        )

        return cast(
            AnalysisGraphState,
            {
                **state,
                "dataset_context": dataset_context.model_dump(),
                "status": "DATASET_PROFILED",
                "warnings": warnings_list,
                "errors": errors,
            },
        )

    except Exception as e:
        errors.append(
            {
                "type": "dataset_context_build_failed",
                "message": str(e),
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "warnings": warnings_list,
                "errors": errors,
            },
        )


if __name__ == "__main__":
    import traceback

    demo_dataset_path = "./data/demo_sales.csv"

    state: AnalysisGraphState = {
        "request_id": "req_local_test_001",
        "session_id": "sess_local_test_001",
        "user_id": "user_local_test_001",
        "dataset_id": "demo_sales_dataset",
        "dataset_path": demo_dataset_path,
        "user_prompt": "请做一份详细且图表丰富的销售分析报告，重点关注区域、产品和时间趋势。",
        "input_config": {
            "language": "zh-CN",
            "output_format": ["markdown"],
        },
        "memory_context": {},
        "normalized_task": {
            "task_type": "reporting",
            "analysis_mode": "reporting",
            "business_goal": "生成可发布的数据分析报告",
            "target_audience": "business_stakeholders",
            "primary_questions": ["时间趋势", "区域差异", "产品结构", "销售表现"],
            "constraints": {
                "language": "zh-CN",
                "prefer_visualization": True,
                "detail_level": "high",
                "desired_output_formats": ["markdown"],
            },
            "ambiguities": [
                {
                    "field": "time_scope",
                    "status": "unspecified",
                    "fallback_policy": "infer_from_dataset",
                }
            ],
            "success_intent": "produce_publishable_analysis_report",
            "normalization_notes": "rule_based_plus_llm_refine",
        },
        "revision_round": 0,
        "max_review_rounds": 2,
        "revision_tasks": [],
        "revision_context": {},
        "execution_mode": "normal",
        "status": "TASK_NORMALIZED",
        "errors": [],
        "warnings": [],
        "degraded_output": False,
    }

    try:
        print("=" * 80)
        print("开始测试 build_dataset_context_node")
        print("=" * 80)
        print(f"数据集路径: {demo_dataset_path}")
        print(f"文件是否存在: {Path(demo_dataset_path).exists()}")
        print()

        result = build_dataset_context_node(state)

        print("=" * 80)
        print("Node 执行完成")
        print("=" * 80)
        print(f"status: {result.get('status')}")
        print()

        if result.get("errors"):
            print("[errors]")
            print(json.dumps(result["errors"], ensure_ascii=False, indent=2))
            print()

        if result.get("warnings"):
            print("[warnings]")
            print(json.dumps(result["warnings"], ensure_ascii=False, indent=2))
            print()

        dataset_context = result.get("dataset_context")
        if not dataset_context:
            print("没有生成 dataset_context")
        else:
            print("[dataset_context 摘要]")
            print(f"dataset_id: {dataset_context.get('dataset_id')}")
            print(f"source_path: {dataset_context.get('source_path')}")
            print(f"candidate_time_columns: {dataset_context.get('candidate_time_columns')}")
            print(f"candidate_measure_columns: {dataset_context.get('candidate_measure_columns')}")
            print(f"candidate_dimension_columns: {dataset_context.get('candidate_dimension_columns')}")
            print(f"candidate_id_columns: {dataset_context.get('candidate_id_columns')}")
            print(f"business_hints: {dataset_context.get('business_hints')}")
            print()

            print("[time_coverage]")
            print(json.dumps(dataset_context.get("time_coverage", {}), ensure_ascii=False, indent=2))
            print()

            print("[data_quality_summary]")
            print(json.dumps(dataset_context.get("data_quality_summary", {}), ensure_ascii=False, indent=2))
            print()

            tables = dataset_context.get("tables", [])
            if tables:
                first_table = tables[0]
                print("[首张表列画像预览]")
                for col in first_table.get("columns", [])[:10]:
                    print(
                        json.dumps(
                            {
                                "name": col.get("name"),
                                "physical_type": col.get("physical_type"),
                                "semantic_type": col.get("semantic_type"),
                                "role_candidates": col.get("role_candidates"),
                                "semantic_confidence": col.get("semantic_confidence"),
                                "null_ratio": col.get("null_ratio"),
                                "unique_ratio": col.get("unique_ratio"),
                                "sample_values": col.get("sample_values"),
                            },
                            ensure_ascii=False,
                        )
                    )
                print()

            output_path = "./tmp_dataset_context_output.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)

            print(f"完整结果已写入: {output_path}")

    except Exception as e:
        print("测试运行失败：")
        print(str(e))
        print(traceback.format_exc())
