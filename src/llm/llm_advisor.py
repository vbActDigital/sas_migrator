import json
from typing import Dict, Optional

from src.utils.logger import get_logger

logger = get_logger("llm_advisor")


class LLMAdvisor:
    def __init__(self, llm_client):
        self.llm = llm_client

    def validate_parser_output(self, sas_code: str, parser_result: Dict) -> Dict:
        prompt = f"""Analyze the SAS code below and validate the parser output.
Identify any missed elements (macros, datasets, PROCs) or incorrect extractions.

SAS CODE:
```sas
{sas_code[:3000]}
```

PARSER OUTPUT:
```json
{json.dumps(parser_result, indent=2)[:2000]}
```

Respond in JSON with keys: "findings" (list of issues), "accuracy_pct" (int), "missed_elements" (list).
"""
        return self._call_and_parse(prompt, "You are a SAS code analysis expert.")

    def review_architecture(self, inventory: Dict, lineage: Dict, complexity: Dict) -> Dict:
        prompt = f"""Review this SAS environment assessment and provide migration recommendations.

INVENTORY SUMMARY:
{json.dumps(inventory, indent=2)[:2000]}

LINEAGE SUMMARY:
Nodes: {len(lineage.get('nodes', []))}, Edges: {len(lineage.get('edges', []))}

COMPLEXITY:
{json.dumps(complexity, indent=2)[:1000]}

Respond in JSON with keys: "strategy" (str), "phases" (list), "risks" (list), "recommendations" (list).
"""
        return self._call_and_parse(prompt, "You are a data migration architect.")

    def enrich_catalog_entry(self, dataset_meta: Dict, context: Dict) -> Dict:
        prompt = f"""Generate a business description and data quality rules for this dataset.

DATASET: {json.dumps(dataset_meta, indent=2)[:2000]}

Respond in JSON with keys: "description" (str), "business_purpose" (str), "quality_rules" (list of str).
"""
        return self._call_and_parse(prompt, "You are a data governance expert.")

    def review_transpiled_code(self, original_sas: str, generated_code: str, target: str) -> Dict:
        prompt = f"""Review this SAS-to-{target} transpilation for correctness and completeness.

ORIGINAL SAS:
```sas
{original_sas[:2000]}
```

GENERATED CODE:
```
{generated_code[:2000]}
```

Respond in JSON with keys: "issues" (list), "suggestions" (list), "correctness_pct" (int).
"""
        return self._call_and_parse(prompt, f"You are a {target} migration expert.")

    def suggest_gap_resolution(self, gap_report: Dict) -> Dict:
        prompt = f"""Suggest resolutions for these SAS migration gaps.

GAPS:
{json.dumps(gap_report, indent=2)[:2000]}

Respond in JSON with keys: "suggestions" (list of dicts with "gap", "resolution", "effort").
"""
        return self._call_and_parse(prompt, "You are a SAS migration expert.")

    def _call_and_parse(self, prompt: str, system_prompt: str) -> Dict:
        response = self.llm.call(prompt, system_prompt=system_prompt)
        if not response:
            return {}
        try:
            # Try to extract JSON from markdown code blocks
            import re
            json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            return json.loads(response)
        except json.JSONDecodeError:
            logger.warning("Failed to parse LLM response as JSON")
            return {"raw_response": response}
