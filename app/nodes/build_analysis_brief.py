from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, cast

from pydantic import ValidationError

from app.config import settings
from app.schemas.brief import (
    AnalysisBrief,
    ChartPolicy,
    ConfidencePolicy,
    ReportStyle,
    RevisionPolicy,
    TablePolicy,
)
from app.schemas.state import AnalysisGraphState
from app.services.llm_service import LLMService


# =========================
# 通用工具
# =========================


def _to_lower_str_list(values: List[Any]) -> List[str]:
    return [str(v).strip().lower() for v in values if str(v).strip()]


def _unique_keep_order(values: List[str]) -> List[str]:
    return list(dict.fromkeys([v for v in values if v]))


def _contains_any(text: str, words: List[str]) -> bool:
    t = text.lower()
    return any(w.lower() in t for w in words)


def _safe_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2, default=str)


# =========================
# 用户意图 / 维度偏好提取（规则）
# 不额外调用 LLM，节省次数
# =========================


_DIMENSION_INTENT_SYNONYMS: Dict[str, List[str]] = {
    "time": ["时间", "趋势", "时序", "日期", "按天", "按月", "按周", "按季度", "时间趋势"],
    "region": ["区域", "地区", "地域", "城市", "省份", "大区", "地理", "区域差异"],
    "product": ["产品", "商品", "品类", "类目", "sku", "产品结构"],
    "customer": ["客户", "用户", "会员", "人群"],
    "channel": ["渠道", "来源", "平台", "来源渠道"],
    "store": ["门店", "店铺", "门店维度"],
    "sales_rep": ["销售员", "销售代表", "销售人员", "业务员", "销售rep"],
    "profit": ["利润", "盈利", "利润率", "毛利", "收益"],
    "anomaly": ["异常", "异常波动", "波动", "突增", "突降"],
}


def _extract_requested_concepts(
    normalized_task: Dict[str, Any],
    user_prompt: str,
) -> List[str]:
    """
    提取用户在任务描述中显式或隐式关心的分析概念。
    返回概念层标签，如 region / product / time / profit / anomaly。
    """
    primary_questions = normalized_task.get("primary_questions", []) or []
    text = " ".join([user_prompt] + [str(x) for x in primary_questions])

    requested: List[str] = []
    for concept, synonyms in _DIMENSION_INTENT_SYNONYMS.items():
        if any(word in text for word in synonyms):
            requested.append(concept)

    return _unique_keep_order(requested)


def _map_requested_concepts_to_columns(
    requested_concepts: List[str],
    dataset_context: Dict[str, Any],
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """
    将用户请求的抽象概念映射到真实列：
    返回：
    - preferred_dimension_columns
    - preferred_time_columns
    - preferred_measure_columns
    - unmatched_requested_concepts
    """
    dims = dataset_context.get("candidate_dimension_columns", []) or []
    times = dataset_context.get("candidate_time_columns", []) or []
    measures = dataset_context.get("candidate_measure_columns", []) or []
    id_cols = set(dataset_context.get("candidate_id_columns", []) or [])

    dims = [d for d in dims if d not in id_cols]

    dim_lower_map = {d.lower(): d for d in dims}
    time_lower_map = {t.lower(): t for t in times}
    measure_lower_map = {m.lower(): m for m in measures}

    preferred_dims: List[str] = []
    preferred_times: List[str] = []
    preferred_measures: List[str] = []
    unmatched: List[str] = []

    def _find_dim_by_keywords(keywords: List[str]) -> List[str]:
        found: List[str] = []
        for raw, original in dim_lower_map.items():
            if any(k.lower() in raw for k in keywords):
                found.append(original)
        return _unique_keep_order(found)

    def _find_time_by_keywords(keywords: List[str]) -> List[str]:
        found: List[str] = []
        for raw, original in time_lower_map.items():
            if any(k.lower() in raw for k in keywords):
                found.append(original)
        return _unique_keep_order(found)

    def _find_measure_by_keywords(keywords: List[str]) -> List[str]:
        found: List[str] = []
        for raw, original in measure_lower_map.items():
            if any(k.lower() in raw for k in keywords):
                found.append(original)
        return _unique_keep_order(found)

    for concept in requested_concepts:
        if concept == "region":
            matched = _find_dim_by_keywords(["region", "area", "区域", "地区", "城市", "省", "市"])
            if matched:
                preferred_dims.extend(matched)
            else:
                unmatched.append(concept)

        elif concept == "product":
            matched = _find_dim_by_keywords(["product", "category", "sku", "item", "产品", "品类", "商品"])
            if matched:
                preferred_dims.extend(matched)
            else:
                unmatched.append(concept)

        elif concept == "customer":
            matched = _find_dim_by_keywords(["customer", "client", "user", "member", "客户", "用户", "会员"])
            if matched:
                preferred_dims.extend(matched)
            else:
                unmatched.append(concept)

        elif concept == "channel":
            matched = _find_dim_by_keywords(["channel", "source", "渠道", "来源", "平台"])
            if matched:
                preferred_dims.extend(matched)
            else:
                unmatched.append(concept)

        elif concept == "store":
            matched = _find_dim_by_keywords(["store", "shop", "门店", "店铺"])
            if matched:
                preferred_dims.extend(matched)
            else:
                unmatched.append(concept)

        elif concept == "sales_rep":
            matched = _find_dim_by_keywords(["sales", "rep", "owner", "销售", "业务员", "销售员", "销售代表"])
            if matched:
                preferred_dims.extend(matched)
            else:
                unmatched.append(concept)

        elif concept == "time":
            matched = _find_time_by_keywords(["date", "time", "日期", "时间"])
            if matched:
                preferred_times.extend(matched)
            elif times:
                preferred_times.extend(times[:1])
            else:
                unmatched.append(concept)

        elif concept == "profit":
            matched = _find_measure_by_keywords(["profit", "margin", "利润", "毛利", "盈利"])
            if matched:
                preferred_measures.extend(matched)
            else:
                # 如果没有现成利润列，但有多个 measure，可交给后续主题兜底
                unmatched.append(concept)

        elif concept == "anomaly":
            # 异常不是列概念，属于分析主题，不在此映射
            continue

    return (
        _unique_keep_order(preferred_dims),
        _unique_keep_order(preferred_times),
        _unique_keep_order(preferred_measures),
        _unique_keep_order(unmatched),
    )


# =========================
# 基线 brief 规则逻辑
# =========================


def _select_must_cover_topics(
    normalized_task: Dict[str, Any],
    dataset_context: Dict[str, Any],
    requested_concepts: List[str],
) -> List[str]:
    topics: List[str] = []

    primary_questions = normalized_task.get("primary_questions", []) or []
    candidate_time_columns = dataset_context.get("candidate_time_columns", []) or []
    candidate_measure_columns = dataset_context.get("candidate_measure_columns", []) or []
    candidate_dimension_columns = dataset_context.get("candidate_dimension_columns", []) or []
    time_coverage = dataset_context.get("time_coverage", {}) or {}

    pq_text = " ".join([str(x) for x in primary_questions])

    # 1) measure 存在时，overall_performance 必须保留
    if candidate_measure_columns:
        topics.append("overall_performance")

    # 2) 时间趋势：用户要求 + 数据支持
    if candidate_time_columns and (
        "time" in requested_concepts
        or "时间趋势" in pq_text
        or "趋势" in pq_text
        or len(candidate_time_columns) > 0
    ):
        topics.append("time_trend")

    # 3) 区域
    if (
        "region" in requested_concepts
        or any("region" in c.lower() or "区域" in c or "地区" in c for c in candidate_dimension_columns)
    ):
        topics.append("regional_comparison")

    # 4) 产品
    if (
        "product" in requested_concepts
        or any(
            "product" in c.lower() or "category" in c.lower() or "产品" in c or "品类" in c
            for c in candidate_dimension_columns
        )
    ):
        topics.append("product_mix")

    # 5) 利润
    if (
        "profit" in requested_concepts
        or any(_contains_any(m, ["profit", "margin", "利润", "毛利"]) for m in candidate_measure_columns)
    ):
        topics.append("profitability_summary")

    # 6) 异常
    if "anomaly" in requested_concepts or "异常波动" in primary_questions:
        topics.append("anomaly_scan")

    # 7) 小样本 / 短期数据弱化过多主题
    granularity_candidates = time_coverage.get("granularity_candidates", []) or []
    if len(candidate_measure_columns) > 0 and len(candidate_dimension_columns) == 0 and "time_trend" not in topics:
        topics.append("overall_performance")

    if not topics:
        topics.append("overall_performance")

    return _unique_keep_order(topics)


def _normalize_final_must_cover_topics(
    must_cover_topics: List[str],
    dataset_context: Dict[str, Any],
) -> List[str]:
    final_topics = list(must_cover_topics or [])
    candidate_measure_columns = dataset_context.get("candidate_measure_columns", []) or []

    if candidate_measure_columns and "overall_performance" not in final_topics:
        final_topics.insert(0, "overall_performance")

    if not final_topics:
        final_topics.append("overall_performance")

    return _unique_keep_order(final_topics)


def _select_optional_topics(
    dataset_context: Dict[str, Any],
    requested_concepts: List[str],
) -> List[str]:
    optional: List[str] = []

    dims = dataset_context.get("candidate_dimension_columns", []) or []
    measures = dataset_context.get("candidate_measure_columns", []) or []

    if "customer" in requested_concepts or any("customer" in c.lower() or "客户" in c for c in dims):
        optional.append("customer_breakdown")

    if any("discount" in m.lower() or "折扣" in m for m in measures):
        optional.append("discount_impact")

    if any("sales" in c.lower() or "rep" in c.lower() or "销售" in c for c in dims):
        optional.append("sales_rep_breakdown")

    if "channel" in requested_concepts or any("channel" in c.lower() or "渠道" in c for c in dims):
        optional.append("channel_breakdown")

    return _unique_keep_order(optional)


def _select_recommended_metrics(
    dataset_context: Dict[str, Any],
    requested_concepts: List[str],
    normalized_task: Dict[str, Any],
) -> List[str]:
    measures = dataset_context.get("candidate_measure_columns", []) or []
    if not measures:
        return []

    user_prompt = str(normalized_task.get("business_goal", "")) + " " + " ".join(
        [str(x) for x in normalized_task.get("primary_questions", []) or []]
    )

    selected: List[str] = []
    lower_map = {m.lower(): m for m in measures}

    # 利润优先
    if "profit" in requested_concepts:
        for p in ["profit", "margin", "利润", "毛利"]:
            for raw, original in lower_map.items():
                if p in raw and original not in selected:
                    selected.append(original)

    # 销售报告优先级
    priority_order = [
        "revenue",
        "sales",
        "gmv",
        "amount",
        "profit",
        "margin",
        "quantity",
        "volume",
        "cost",
        "discount",
        "收入",
        "销售",
        "金额",
        "利润",
        "毛利",
        "销量",
        "成本",
        "折扣",
    ]

    for p in priority_order:
        for raw, original in lower_map.items():
            if p in raw and original not in selected:
                selected.append(original)

    # 用户 business_goal 文本增强排序
    for raw, original in lower_map.items():
        if raw in user_prompt.lower() and original not in selected:
            selected.append(original)

    for m in measures:
        if m not in selected:
            selected.append(m)

    return selected[:6]


def _select_recommended_dimensions(
    dataset_context: Dict[str, Any],
    requested_concepts: List[str],
    preferred_dimension_columns: List[str],
    preferred_time_columns: List[str],
) -> List[str]:
    dims = dataset_context.get("candidate_dimension_columns", []) or []
    id_cols = set(dataset_context.get("candidate_id_columns", []) or [])
    times = dataset_context.get("candidate_time_columns", []) or []

    dims = [d for d in dims if d not in id_cols]

    selected: List[str] = []

    # 1) 用户明确偏好的维度优先
    selected.extend([d for d in preferred_dimension_columns if d in dims and d not in selected])

    # 2) 用户明确偏好的时间列也纳入 recommended_dimensions，方便后续 planner 使用
    selected.extend([t for t in preferred_time_columns if t in times and t not in selected])

    # 3) 如果用户关心区域 / 产品，则按概念优先补充
    if "region" in requested_concepts:
        for d in dims:
            if _contains_any(d, ["region", "area", "区域", "地区", "城市", "省"]):
                selected.append(d)

    if "product" in requested_concepts:
        for d in dims:
            if _contains_any(d, ["product", "category", "sku", "产品", "品类", "商品"]):
                selected.append(d)

    if "customer" in requested_concepts:
        for d in dims:
            if _contains_any(d, ["customer", "client", "member", "客户", "会员"]):
                selected.append(d)

    if "channel" in requested_concepts:
        for d in dims:
            if _contains_any(d, ["channel", "source", "渠道", "来源", "平台"]):
                selected.append(d)

    # 4) 常规优先级
    for d in dims:
        if _contains_any(d, ["region", "area", "区域", "地区"]):
            selected.append(d)

    for d in dims:
        if _contains_any(d, ["product", "category", "sku", "产品", "品类"]):
            selected.append(d)

    # 5) 兜底补齐
    for d in dims:
        if d not in selected:
            selected.append(d)

    # 6) 时间列补齐，但放后面
    for t in times:
        if t not in selected:
            selected.append(t)

    return _unique_keep_order(selected)[:6]


def _build_chart_policy(
    normalized_task: Dict[str, Any],
    dataset_context: Dict[str, Any],
    must_cover_topics: List[str],
) -> ChartPolicy:
    detail_level = normalized_task.get("constraints", {}).get("detail_level", "high")
    has_time = len(dataset_context.get("candidate_time_columns", []) or []) > 0
    has_dims = len(dataset_context.get("candidate_dimension_columns", []) or []) > 0
    prefer_visualization = normalized_task.get("constraints", {}).get("prefer_visualization", True)

    if detail_level == "high":
        target_range = [6, 10] if prefer_visualization else [4, 7]
        max_total = 10 if prefer_visualization else 7
    elif detail_level == "medium":
        target_range = [4, 7]
        max_total = 7
    else:
        target_range = [2, 4]
        max_total = 4

    preferred: List[str] = ["bar"]

    if has_time or "time_trend" in must_cover_topics:
        preferred.insert(0, "line")

    if has_dims:
        preferred.extend(["stacked_bar", "heatmap"])

    if "product_mix" in must_cover_topics:
        preferred.append("treemap")

    return ChartPolicy(
        target_chart_range=target_range,
        max_total_charts=max_total,
        max_similar_chart_per_metric=2,
        preferred_chart_types=_unique_keep_order(preferred),
        avoid_chart_types=["low_information_pie", "duplicate_histogram"],
    )


def _build_table_policy(
    dataset_context: Dict[str, Any],
    must_cover_topics: List[str],
) -> TablePolicy:
    must_have = ["summary_kpi_table"]

    dims = dataset_context.get("candidate_dimension_columns", []) or []

    if any("region" in d.lower() or "区域" in d or "地区" in d for d in dims):
        must_have.append("regional_comparison_table")

    if "product_mix" in must_cover_topics:
        must_have.append("product_mix_table")

    if "time_trend" in must_cover_topics:
        must_have.append("trend_summary_table")

    return TablePolicy(
        max_total_tables=6,
        must_have_tables=_unique_keep_order(must_have),
    )


def _build_completion_criteria(
    must_cover_topics: List[str],
    dataset_context: Dict[str, Any],
) -> List[str]:
    criteria = [
        "所有 must_cover_topics 均被覆盖",
        "每个核心结论可追溯到图表或表格证据",
        "报告需包含执行摘要、主体分析、风险与建议",
    ]

    if "time_trend" in must_cover_topics:
        criteria.append("至少包含一张有效的时间趋势图")

    if "regional_comparison" in must_cover_topics:
        criteria.append("至少包含一组区域对比分析")

    if "product_mix" in must_cover_topics:
        criteria.append("至少包含一组产品结构分析")

    time_coverage = dataset_context.get("time_coverage", {}) or {}
    if time_coverage.get("min") and time_coverage.get("max"):
        criteria.append("时间范围说明需与数据覆盖区间一致")

    return _unique_keep_order(criteria)


def _build_must_not_do() -> List[str]:
    return [
        "未经证据支持的因果推断",
        "生成重复信息量图表",
        "使用不在数据中的业务背景作强结论",
        "在 Writer 阶段新增未经 evidence_pack 支持的事实",
        "把 identifier 列直接当作核心分析维度展开",
    ]


def _build_rule_based_brief_notes(
    requested_concepts: List[str],
    preferred_dimension_columns: List[str],
    preferred_time_columns: List[str],
    unmatched_requested_concepts: List[str],
) -> str:
    notes: List[str] = ["rule_based_baseline_brief"]

    if requested_concepts:
        notes.append(f"requested_concepts={requested_concepts}")

    if preferred_dimension_columns:
        notes.append(f"preferred_dimension_columns={preferred_dimension_columns}")

    if preferred_time_columns:
        notes.append(f"preferred_time_columns={preferred_time_columns}")

    if unmatched_requested_concepts:
        notes.append(f"unmatched_requested_concepts={unmatched_requested_concepts}")

    return " | ".join(notes)


def _sanitize_brief_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if "version" not in payload:
        payload["version"] = 1

    if "brief_id" not in payload:
        payload["brief_id"] = "brief_001"

    report_style = payload.get("report_style", {})
    if not isinstance(report_style, dict):
        report_style = {}

    if report_style.get("language") not in ["zh-CN", "en"]:
        report_style["language"] = "zh-CN"
    if report_style.get("tone") not in ["professional", "concise", "executive"]:
        report_style["tone"] = "professional"
    if report_style.get("detail_level") not in ["low", "medium", "high"]:
        report_style["detail_level"] = "high"

    payload["report_style"] = report_style
    return payload


# =========================
# LLM refine（仅一次调用）
# =========================


def llm_refine_analysis_brief(
    *,
    normalized_task: Dict[str, Any],
    dataset_context: Dict[str, Any],
    baseline_brief: AnalysisBrief,
    requested_concepts: List[str],
    preferred_dimension_columns: List[str],
    preferred_time_columns: List[str],
    preferred_measure_columns: List[str],
    unmatched_requested_concepts: List[str],
) -> AnalysisBrief:
    llm = LLMService()

    system_prompt = """
你是一个分析执行合同整理器。
你的职责是根据 normalized_task、dataset_context、用户偏好信息、baseline_brief，对 baseline_brief 做温和修正与补全。

严格要求：
1. 只能基于 baseline_brief 做小幅修正，不要发散。
2. 不要发明数据集中不存在的字段、指标、维度。
3. 输出必须是一个 JSON 对象。
4. 不要输出 markdown，不要输出解释。
5. 所有字段名必须与 baseline_brief 一致。
6. must_cover_topics 应该少而关键，不要过多扩展。
7. recommended_metrics / recommended_dimensions 必须来自 dataset_context 中已有候选。
8. chart_policy / table_policy 只允许做小幅优化。
9. 不允许删除 revision_policy / confidence_policy。
10. 不要把 identifier 列放入 recommended_dimensions。
11. 如果存在 measure 列，must_cover_topics 中应保留 overall_performance。
12. 如果用户明确要求“区域/产品/时间”等维度，应优先在 recommended_dimensions 和 must_cover_topics 中体现。
13. 如果用户要求的概念在数据中找不到，不要捏造对应字段，只能在 brief_notes 中温和提示。
""".strip()

    user_payload = {
        "normalized_task": normalized_task,
        "dataset_context": dataset_context,
        "user_preferences": {
            "requested_concepts": requested_concepts,
            "preferred_dimension_columns": preferred_dimension_columns,
            "preferred_time_columns": preferred_time_columns,
            "preferred_measure_columns": preferred_measure_columns,
            "unmatched_requested_concepts": unmatched_requested_concepts,
        },
        "baseline_brief": baseline_brief.model_dump(),
        "instruction": "请输出修正后的 analysis_brief JSON。",
    }

    result = llm.json_invoke(
        system_prompt=system_prompt,
        user_prompt=_safe_json_dumps(user_payload),
        temperature=0.1,
    )

    result = _sanitize_brief_payload(result)
    return AnalysisBrief.model_validate(result)


# =========================
# 主 node
# =========================


def build_analysis_brief_node(state: AnalysisGraphState) -> AnalysisGraphState:
    normalized_task = state.get("normalized_task")
    dataset_context = state.get("dataset_context")

    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    if not normalized_task:
        errors.append(
            {
                "type": "missing_normalized_task",
                "message": "normalized_task is required before building analysis_brief.",
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "errors": errors,
                "warnings": warnings,
            },
        )

    if not dataset_context or dataset_context.get("status") == "stub":
        errors.append(
            {
                "type": "missing_dataset_context",
                "message": "dataset_context is required before building analysis_brief.",
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "errors": errors,
                "warnings": warnings,
            },
        )

    try:
        user_prompt = str(state.get("user_prompt", ""))

        requested_concepts = _extract_requested_concepts(normalized_task, user_prompt)
        (
            preferred_dimension_columns,
            preferred_time_columns,
            preferred_measure_columns,
            unmatched_requested_concepts,
        ) = _map_requested_concepts_to_columns(requested_concepts, dataset_context)

        must_cover_topics = _select_must_cover_topics(
            normalized_task=normalized_task,
            dataset_context=dataset_context,
            requested_concepts=requested_concepts,
        )
        optional_topics = _select_optional_topics(
            dataset_context=dataset_context,
            requested_concepts=requested_concepts,
        )
        recommended_metrics = _select_recommended_metrics(
            dataset_context=dataset_context,
            requested_concepts=requested_concepts,
            normalized_task=normalized_task,
        )
        recommended_dimensions = _select_recommended_dimensions(
            dataset_context=dataset_context,
            requested_concepts=requested_concepts,
            preferred_dimension_columns=preferred_dimension_columns,
            preferred_time_columns=preferred_time_columns,
        )

        baseline_brief = AnalysisBrief(
            brief_id="brief_001",
            version=1,
            task_type=normalized_task.get("task_type", "unknown"),
            business_goal=normalized_task.get("business_goal", "生成分析报告"),
            target_audience=normalized_task.get("target_audience", "unknown"),
            report_style=ReportStyle(
                language=normalized_task.get("constraints", {}).get("language", "zh-CN"),
                tone="professional",
                detail_level=normalized_task.get("constraints", {}).get("detail_level", "high"),
            ),
            must_cover_topics=must_cover_topics,
            optional_topics=optional_topics,
            must_not_do=_build_must_not_do(),
            recommended_metrics=recommended_metrics,
            recommended_dimensions=recommended_dimensions,
            chart_policy=_build_chart_policy(
                normalized_task=normalized_task,
                dataset_context=dataset_context,
                must_cover_topics=must_cover_topics,
            ),
            table_policy=_build_table_policy(
                dataset_context=dataset_context,
                must_cover_topics=must_cover_topics,
            ),
            completion_criteria=_build_completion_criteria(
                must_cover_topics=must_cover_topics,
                dataset_context=dataset_context,
            ),
            confidence_policy=ConfidencePolicy(
                default_claim_level="descriptive_or_associational",
                forbid_causal_language_without_evidence=True,
            ),
            revision_policy=RevisionPolicy(
                max_review_rounds=state.get("max_review_rounds", 2),
                must_fix_first=True,
            ),
            brief_notes=_build_rule_based_brief_notes(
                requested_concepts=requested_concepts,
                preferred_dimension_columns=preferred_dimension_columns,
                preferred_time_columns=preferred_time_columns,
                unmatched_requested_concepts=unmatched_requested_concepts,
            ),
        )

        final_brief = baseline_brief

        if settings.llm_enable_refine:
            try:
                final_brief = llm_refine_analysis_brief(
                    normalized_task=normalized_task,
                    dataset_context=dataset_context,
                    baseline_brief=baseline_brief,
                    requested_concepts=requested_concepts,
                    preferred_dimension_columns=preferred_dimension_columns,
                    preferred_time_columns=preferred_time_columns,
                    preferred_measure_columns=preferred_measure_columns,
                    unmatched_requested_concepts=unmatched_requested_concepts,
                )
                final_brief = final_brief.model_copy(
                    update={"brief_notes": "rule_based_plus_llm_refine"}
                )
            except ValidationError as e:
                warnings.append(
                    {
                        "type": "analysis_brief_validation_fallback",
                        "message": f"LLM refine 输出未通过 schema 校验，已回退到 baseline brief。details={str(e)}",
                    }
                )
                final_brief = baseline_brief
            except Exception as e:
                warnings.append(
                    {
                        "type": "analysis_brief_llm_fallback",
                        "message": f"LLM refine 调用失败，已回退到 baseline brief。details={str(e)}",
                    }
                )
                final_brief = baseline_brief

        # 最终强约束
        final_brief = final_brief.model_copy(
            update={
                "must_cover_topics": _normalize_final_must_cover_topics(
                    final_brief.must_cover_topics,
                    dataset_context,
                ),
                "recommended_dimensions": [
                    d
                    for d in final_brief.recommended_dimensions
                    if d not in set(dataset_context.get("candidate_id_columns", []) or [])
                ],
            }
        )

        return cast(
            AnalysisGraphState,
            {
                **state,
                "analysis_brief": final_brief.model_dump(),
                "status": "BRIEF_READY",
                "warnings": warnings,
                "errors": errors,
            },
        )

    except Exception as e:
        errors.append(
            {
                "type": "analysis_brief_build_failed",
                "message": str(e),
            }
        )
        return cast(
            AnalysisGraphState,
            {
                **state,
                "status": "FAILED",
                "warnings": warnings,
                "errors": errors,
            },
        )


# =========================
# main 测试
# =========================

if __name__ == "__main__":
    import traceback

    state: AnalysisGraphState = {
        "request_id": "req_004",
        "session_id": "sess_004",
        "user_id": "user_004",
        "dataset_id": "ds_004",
        "dataset_path": "./data/demo_sales.csv",
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
        "dataset_context": {
            "dataset_id": "ds_004",
            "profile_version": "1.0",
            "source_path": "./data/demo_sales.csv",
            "tables": [
                {
                    "table_name": "demo_sales.csv",
                    "row_count": 10,
                    "column_count": 7,
                    "columns": [
                        {
                            "name": "order_id",
                            "physical_type": "int",
                            "semantic_type": "id",
                            "null_ratio": 0.0,
                            "unique_ratio": 1.0,
                            "non_null_count": 10,
                            "unique_count": 10,
                            "sample_values": ["1", "2", "3", "4", "5"],
                            "role_candidates": ["identifier"],
                            "semantic_confidence": 1.0,
                        },
                        {
                            "name": "order_date",
                            "physical_type": "datetime",
                            "semantic_type": "date",
                            "null_ratio": 0.0,
                            "unique_ratio": 0.5,
                            "non_null_count": 10,
                            "unique_count": 5,
                            "sample_values": [
                                "2026-03-01",
                                "2026-03-02",
                                "2026-03-03",
                                "2026-03-04",
                                "2026-03-05",
                            ],
                            "role_candidates": ["time_dimension"],
                            "semantic_confidence": 1.0,
                        },
                        {
                            "name": "region",
                            "physical_type": "string",
                            "semantic_type": "category",
                            "null_ratio": 0.0,
                            "unique_ratio": 0.4,
                            "non_null_count": 10,
                            "unique_count": 4,
                            "sample_values": ["East", "West", "South", "North"],
                            "role_candidates": ["business_dimension", "geo_dimension"],
                            "semantic_confidence": 1.0,
                        },
                        {
                            "name": "category",
                            "physical_type": "string",
                            "semantic_type": "category",
                            "null_ratio": 0.0,
                            "unique_ratio": 0.3,
                            "non_null_count": 10,
                            "unique_count": 3,
                            "sample_values": ["Electronics", "Home", "Beauty"],
                            "role_candidates": ["business_dimension", "product_dimension"],
                            "semantic_confidence": 1.0,
                        },
                        {
                            "name": "gmv",
                            "physical_type": "float",
                            "semantic_type": "metric",
                            "null_ratio": 0.1,
                            "unique_ratio": 0.9,
                            "non_null_count": 9,
                            "unique_count": 9,
                            "sample_values": ["1200.0", "500.0", "1800.0", "300.0", "700.0"],
                            "role_candidates": ["measure"],
                            "semantic_confidence": 1.0,
                        },
                        {
                            "name": "cost",
                            "physical_type": "float",
                            "semantic_type": "metric",
                            "null_ratio": 0.1,
                            "unique_ratio": 0.9,
                            "non_null_count": 9,
                            "unique_count": 9,
                            "sample_values": ["800.0", "300.0", "1200.0", "120.0", "420.0"],
                            "role_candidates": ["measure"],
                            "semantic_confidence": 1.0,
                        },
                        {
                            "name": "user_id",
                            "physical_type": "string",
                            "semantic_type": "id",
                            "null_ratio": 0.0,
                            "unique_ratio": 1.0,
                            "non_null_count": 10,
                            "unique_count": 10,
                            "sample_values": ["u1", "u2", "u3", "u4", "u5"],
                            "role_candidates": ["identifier", "customer_dimension"],
                            "semantic_confidence": 1.0,
                        },
                    ],
                }
            ],
            "candidate_time_columns": ["order_date"],
            "candidate_measure_columns": ["gmv", "cost"],
            "candidate_dimension_columns": ["region", "category"],
            "candidate_id_columns": ["order_id", "user_id"],
            "data_quality_summary": {
                "missingness": [
                    {"column": "gmv", "null_ratio": 0.1},
                    {"column": "cost", "null_ratio": 0.1},
                ],
                "high_cardinality_columns": [],
                "potential_outliers": [],
                "duplicate_rows_ratio": 0.0,
            },
            "time_coverage": {
                "min": "2026-03-01",
                "max": "2026-03-05",
                "granularity_candidates": ["day"],
            },
            "business_hints": [
                "订单日期可用于分析时间趋势，如日销售变化",
                "区域维度适合分析地理分布差异",
                "产品类别维度可分析销售结构",
                "GMV和成本是核心销售指标，可计算利润",
                "用户ID可用于客户分析，但唯一性高",
                "数据集覆盖5天，适合短期趋势分析",
            ],
            "warnings": [],
        },
        "revision_round": 0,
        "max_review_rounds": 2,
        "revision_tasks": [],
        "revision_context": {},
        "execution_mode": "normal",
        "status": "DATASET_PROFILED",
        "errors": [],
        "warnings": [],
        "degraded_output": False,
    }

    try:
        print("=" * 80)
        print("开始测试 build_analysis_brief_node")
        print("=" * 80)
        print(f"数据集路径: {state.get('dataset_path')}")
        print(f"文件是否存在: {Path(str(state.get('dataset_path'))).exists()}")
        print()

        result = build_analysis_brief_node(state)

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

        analysis_brief = result.get("analysis_brief")
        if not analysis_brief:
            print("没有生成 analysis_brief")
        else:
            print("[analysis_brief 摘要]")
            print(f"brief_id: {analysis_brief.get('brief_id')}")
            print(f"task_type: {analysis_brief.get('task_type')}")
            print(f"business_goal: {analysis_brief.get('business_goal')}")
            print(f"target_audience: {analysis_brief.get('target_audience')}")
            print(f"must_cover_topics: {analysis_brief.get('must_cover_topics')}")
            print(f"optional_topics: {analysis_brief.get('optional_topics')}")
            print(f"recommended_metrics: {analysis_brief.get('recommended_metrics')}")
            print(f"recommended_dimensions: {analysis_brief.get('recommended_dimensions')}")
            print(f"brief_notes: {analysis_brief.get('brief_notes')}")
            print()

            print("[完整 analysis_brief]")
            print(json.dumps(analysis_brief, ensure_ascii=False, indent=2, default=str))
            print()

        output_path = "./tmp_analysis_brief_output.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)

        print(f"完整结果已写入: {output_path}")

    except Exception as e:
        print("测试运行失败：")
        print(str(e))
        print(traceback.format_exc())