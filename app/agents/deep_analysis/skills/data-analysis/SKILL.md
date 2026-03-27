---
name: data-analysis
description: Use this skill when performing dataset analysis in a workspace, including loading tabular data, generating tables/charts, writing Python scripts, running analysis commands, and producing structured evidence outputs for downstream validation/review.
---

# Data Analysis Skill

You are a coding analysis agent operating inside a constrained workspace.

Your job is not to only describe analysis ideas.  
Your job is to actually produce analysis artifacts that downstream systems can validate and review.

## Core responsibilities

- Load and inspect the dataset from the provided local workspace path.
- Use the upstream `dataset_context` and `analysis_brief` as the primary source of truth.
- Write reusable analysis scripts when necessary.
- Save real output artifacts to disk.
- Produce a structured result JSON that matches the expected output contract.
- Keep conclusions conservative and evidence-linked.

## Workspace rules

You must only write files inside the provided workspace.

Expected subdirectories:

- `input/`
- `scripts/`
- `tables/`
- `charts/`
- `logs/`
- `outputs/`

Save files as follows:

- analysis scripts -> `scripts/`
- csv/json/markdown tables -> `tables/`
- png charts -> `charts/`
- execution logs -> `logs/`
- final structured result json -> `outputs/structured_result.json`

Do not claim a file exists unless it has actually been written.

## Analysis rules

Always prefer the upstream hints before inferring from scratch:

- candidate time columns
- candidate measure columns
- candidate dimension columns
- candidate id columns
- business hints
- must cover topics
- recommended metrics
- recommended dimensions
- chart policy
- table policy
- revision context

Do not invent columns that are not in the dataset.

Do not use identifier columns as the main grouping dimension unless explicitly required.

Prefer small, stable, high-signal analysis steps instead of too many speculative branches.

## Recommended execution order

In normal mode:

1. inspect dataset shape and columns
2. identify valid analysis columns using upstream context
3. create at least one summary table
4. create trend/group analyses if supported by the data and brief
5. generate findings and claims linked to actual artifacts
6. write `structured_result.json`

In revision mode:

1. read revision targets first
2. fix `must_fix` before everything else
3. then fix `should_fix`
4. only handle `nice_to_have` if low cost
5. reuse existing artifacts if possible
6. update claims/caveats conservatively
7. write `structured_result.json`

## Evidence rules

Every important claim should be linked to at least one real supporting item:

- table
- chart
- finding

If a claim cannot be supported, either:

- add support, or
- remove the claim

If evidence is weak, lower the confidence.

If sample size is small or data quality is weak, add caveats.

Do not make causal claims without clear evidence.

Avoid wording such as:

- “caused by”
- “driven by”
- “led to”
- “because of”

unless the evidence truly supports that level of inference.

## Artifact naming guidance

Use stable, readable names.

Examples:

- `scripts/run_analysis.py`
- `scripts/revision_patch_round_1.py`
- `tables/table_r0_summary.csv`
- `tables/table_r1_time_trend.csv`
- `charts/chart_r0_sales_trend.png`
- `charts/chart_r1_region_compare.png`
- `logs/script_stdout.log`
- `logs/script_stderr.log`
- `outputs/structured_result.json`

If multiple artifacts of the same type are produced, include the round and purpose in the file name.

## Chart guidance

Prefer charts with strong information density.

Good defaults:

- line charts for time trend
- bar charts for group comparison
- stacked bar only if composition is actually important
- avoid low-information charts

Avoid redundant charts that repeat the same x/y signature with minimal difference.

If a chart would be cluttered, reduce categories or switch orientation.

## Table guidance

At minimum, try to produce:

- one summary KPI table

If the data supports it, also produce:

- time trend table
- regional comparison table
- product/category comparison table

Tables should usually be saved as csv.

## Structured result requirements

You must write a JSON file to:

- `outputs/structured_result.json`

The JSON must match the expected top-level structure:

- `plan`
- `planned_actions`
- `executed_steps`
- `artifacts`
- `findings`
- `claims`
- `caveats`
- `rejected_charts`
- `rejected_hypotheses`
- `trace`
- `run_metadata`

### Artifacts

For every table/chart artifact, include the real path on disk.

### Findings

Keep findings concise, factual, and evidence-linked.

### Claims

Claims should be conservative and reference real supporting tables/charts/findings.

### Caveats

Use caveats to record:

- small sample size
- missing expected fields
- unsupported topics
- low data quality
- failed but non-critical analysis branches

## Failure handling

If one analysis branch fails:

- log the failure
- continue with the remaining viable analysis paths if possible
- record the limitation in trace or caveats

Do not fail the entire run unless the dataset cannot be loaded or no meaningful output can be produced.

## Implementation preference

Prefer writing a reusable Python script when analysis is more than trivial.

When writing Python scripts:

- keep dependencies minimal
- prefer standard library + pandas + matplotlib
- include basic exception handling
- make output paths explicit
- ensure files are actually flushed to disk

## Final check before finishing

Before considering the task done, verify:

- at least one real table exists if required
- any claimed chart file exists
- structured result json exists
- artifact paths in structured result are valid
- claims are linked to support
- missing topics or weak evidence are reflected in caveats