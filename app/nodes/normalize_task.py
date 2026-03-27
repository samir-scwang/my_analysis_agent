from __future__ import annotations

from typing import Any, Dict, List, cast

from pydantic import ValidationError

from app.config import settings
from app.schemas.state import AnalysisGraphState
from app.schemas.task import (
    AmbiguityItem,
    NormalizedTask,
    TaskConstraints,
)
from app.services.llm_service import LLMService


def _infer_task_type(user_prompt: str) -> str:
    if any(k in user_prompt for k in ["诊断", "原因", "为什么", "异常原因"]):
        return "diagnostic_analysis"
    if any(k in user_prompt for k in ["探索", "探索性", "发现", "看看"]):
        return "exploratory_analysis"
    if any(k in user_prompt for k in ["对比", "比较"]):
        return "comparative_analysis"
    if any(k in user_prompt for k in ["报告", "汇报", "总结"]):
        return "reporting"
    if any(k in user_prompt for k in ["分析", "趋势", "结构"]):
        return "descriptive_analysis"
    return "unknown"


def _infer_audience(user_prompt: str) -> str:
    if any(k in user_prompt for k in ["管理层", "高层", "老板", "高管"]):
        return "executives"
    if any(k in user_prompt for k in ["业务方", "业务团队", "运营团队", "销售团队"]):
        return "business_stakeholders"
    if any(k in user_prompt for k in ["分析师", "数据团队"]):
        return "analysts"
    if any(k in user_prompt for k in ["技术团队", "工程团队"]):
        return "technical_team"
    return "unknown"


def _infer_detail_level(user_prompt: str) -> str:
    if any(k in user_prompt for k in ["详细", "深入", "完整", "丰富"]):
        return "high"
    if any(k in user_prompt for k in ["简要", "简洁", "概览"]):
        return "low"
    return "medium"


def _infer_visual_preference(user_prompt: str) -> bool:
    return any(k in user_prompt for k in ["图", "图表", "可视化", "dashboard", "报告"])


def _extract_primary_questions(user_prompt: str) -> List[str]:
    questions: List[str] = []

    keyword_map = {
        "时间趋势": ["趋势", "时间", "按月", "按周", "同比", "环比"],
        "区域差异": ["区域", "地区", "region", "大区"],
        "产品结构": ["产品", "品类", "category", "sku"],
        "利润表现": ["利润", "profit", "margin", "毛利"],
        "销售表现": ["销售", "收入", "revenue", "gmv"],
        "异常波动": ["异常", "波动", "下滑", "增长原因"],
    }

    low = user_prompt.lower()
    for label, keys in keyword_map.items():
        if any((k in low) or (k in user_prompt) for k in keys):
            questions.append(label)

    if not questions:
        questions = ["整体业务表现如何"]

    return questions


def _build_ambiguities(user_prompt: str) -> List[AmbiguityItem]:
    ambiguities: List[AmbiguityItem] = []

    if not any(k in user_prompt for k in ["本月", "本周", "季度", "Q", "年", "最近", "过去", "202", "2024", "2025", "2026"]):
        ambiguities.append(
            AmbiguityItem(
                field="time_scope",
                status="unspecified",
                fallback_policy="infer_from_dataset",
            )
        )

    if not any(k in user_prompt for k in ["管理层", "高层", "高管", "业务方", "分析师", "技术团队"]):
        ambiguities.append(
            AmbiguityItem(
                field="target_audience",
                status="unspecified",
                fallback_policy="default_business_stakeholders",
            )
        )

    return ambiguities


def rule_based_normalize(
    user_prompt: str,
    input_config: Dict[str, Any] | None = None,
) -> NormalizedTask:
    input_config = input_config or {}

    return NormalizedTask(
        task_type=_infer_task_type(user_prompt),
        analysis_mode="reporting",
        business_goal="生成可发布的数据分析报告",
        target_audience=_infer_audience(user_prompt),
        primary_questions=_extract_primary_questions(user_prompt),
        constraints=TaskConstraints(
            language=input_config.get("language", "zh-CN"),
            prefer_visualization=_infer_visual_preference(user_prompt),
            detail_level=_infer_detail_level(user_prompt),
            desired_output_formats=input_config.get("output_format", ["markdown"]),
        ),
        ambiguities=_build_ambiguities(user_prompt),
        success_intent="produce_publishable_analysis_report",
        normalization_notes="rule_based_baseline",
    )
def _sanitize_normalized_task_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    对 LLM 返回的 JSON 做轻量归一化，减少 schema 校验失败。
    """
    allowed_task_types = {
        "descriptive_analysis",
        "diagnostic_analysis",
        "exploratory_analysis",
        "comparative_analysis",
        "reporting",
        "unknown",
    }

    allowed_audiences = {
        "business_stakeholders",
        "executives",
        "analysts",
        "technical_team",
        "general",
        "unknown",
    }

    allowed_detail_levels = {"low", "medium", "high"}
    allowed_ambiguity_status = {"unspecified", "conflicting", "low_confidence"}

    # 1) task_type
    if payload.get("task_type") not in allowed_task_types:
        payload["task_type"] = "unknown"

    # 2) target_audience
    if payload.get("target_audience") not in allowed_audiences:
        payload["target_audience"] = "unknown"

    # 3) detail_level
    constraints = payload.get("constraints", {})
    if isinstance(constraints, dict):
        if constraints.get("detail_level") not in allowed_detail_levels:
            constraints["detail_level"] = "medium"
        payload["constraints"] = constraints

    # 4) ambiguities.status
    ambiguities = payload.get("ambiguities", [])
    if isinstance(ambiguities, list):
        sanitized_ambiguities = []
        for item in ambiguities:
            if not isinstance(item, dict):
                continue

            status = item.get("status")
            if status not in allowed_ambiguity_status:
                # 常见值映射
                mapping = {
                    "specified": "low_confidence",
                    "clear": "low_confidence",
                    "certain": "low_confidence",
                    "unknown": "unspecified",
                    "unclear": "unspecified",
                }
                item["status"] = mapping.get(str(status).lower(), "low_confidence")

            sanitized_ambiguities.append(item)

        payload["ambiguities"] = sanitized_ambiguities

    return payload
    
def llm_refine_normalized_task(
    *,
    user_prompt: str,
    baseline_task: NormalizedTask,
) -> NormalizedTask:
    llm = LLMService()

    system_prompt = """
你是一个数据分析任务标准化器。
你的职责是把用户的原始分析请求，修正为稳定、克制、结构化的任务定义。

你只能基于 baseline_task 做修正和补全，不能发散，不能虚构数据事实。

严格要求：
1. 输出必须是一个 JSON 对象。
2. 不要输出 markdown。
3. 不要输出解释。
4. 所有字段名必须与 baseline_task 完全一致。
5. 如果某个字段不需要修改，就保留 baseline_task 中的原值。
6. ambiguities 中每个对象的 status 只能是以下三个值之一：
   - "unspecified"
   - "conflicting"
   - "low_confidence"
   绝对不要输出其他值，比如 "specified"。
7. target_audience 只能是以下之一：
   - "business_stakeholders"
   - "executives"
   - "analysts"
   - "technical_team"
   - "general"
   - "unknown"
8. task_type 只能是以下之一：
   - "descriptive_analysis"
   - "diagnostic_analysis"
   - "exploratory_analysis"
   - "comparative_analysis"
   - "reporting"
   - "unknown"
9. constraints.detail_level 只能是以下之一：
   - "low"
   - "medium"
   - "high"
10. 如果你认为 baseline 中某个 ambiguity 已经消除，不要输出 "specified"；
    要么删除该 ambiguity 项，要么使用允许的枚举值。

请输出最终 JSON。
"""

    user_payload = {
        "user_prompt": user_prompt,
        "baseline_task": baseline_task.model_dump(),
        "instruction": "请输出修正后的 normalized_task JSON。字段名必须与 baseline_task 一致。",
    }

    result = llm.json_invoke(
        system_prompt=system_prompt,
        user_prompt=str(user_payload),
        temperature=0.1,
    )

    result = _sanitize_normalized_task_payload(result)

    refined = NormalizedTask.model_validate(result)
    return refined

def normalize_task_node(state: AnalysisGraphState) -> AnalysisGraphState:
    user_prompt = state["user_prompt"]
    input_config = state.get("input_config", {})

    warnings = list(state.get("warnings", []))
    errors = list(state.get("errors", []))

    baseline = rule_based_normalize(user_prompt=user_prompt, input_config=input_config)
    normalized = baseline

    if settings.llm_enable_refine:
        try:
            normalized = llm_refine_normalized_task(
                user_prompt=user_prompt,
                baseline_task=baseline,
            )
            normalized.normalization_notes = "rule_based_plus_llm_refine"
        except ValidationError as e:
            warnings.append(
                {
                    "type": "normalize_task_validation_fallback",
                    "message": f"LLM refine 输出未通过 schema 校验，已回退到 rule-based。details={str(e)}",
                }
            )
            normalized = baseline
        except Exception as e:
            warnings.append(
                {
                    "type": "normalize_task_llm_fallback",
                    "message": f"LLM refine 调用失败，已回退到 rule-based。details={str(e)}",
                }
            )
            normalized = baseline

    if normalized.task_type == "unknown":
        warnings.append(
            {
                "type": "task_type_low_confidence",
                "message": "无法高置信识别任务类型，后续将依赖 dataset_context 与 brief builder 进一步收束。",
            }
        )

    return cast(
        AnalysisGraphState,
        {
            **state,
            "normalized_task": normalized.model_dump(),
            "status": "TASK_NORMALIZED",
            "warnings": warnings,
            "errors": errors,
        },
    )