"""Unit tests for LLM Advisor (mock-based, no real API calls)."""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.llm.llm_advisor import LLMAdvisor


class MockLLMClient:
    def __init__(self, response="{}"):
        self._response = response
        self.calls = []

    @property
    def is_available(self):
        return True

    def call(self, prompt, system_prompt=None, model_tier="balanced",
             temperature=0.3, max_tokens=4000):
        self.calls.append({"prompt": prompt, "system_prompt": system_prompt})
        return self._response


class TestLLMAdvisor:
    def test_validate_parser_output(self):
        client = MockLLMClient('{"findings": [], "accuracy_pct": 95, "missed_elements": []}')
        advisor = LLMAdvisor(client)
        result = advisor.validate_parser_output("DATA a; SET b; RUN;", {"filename": "test.sas"})
        assert "findings" in result
        assert len(client.calls) == 1

    def test_review_architecture(self):
        client = MockLLMClient('{"strategy": "phased", "phases": [], "risks": [], "recommendations": []}')
        advisor = LLMAdvisor(client)
        result = advisor.review_architecture({}, {"nodes": [], "edges": []}, {})
        assert "strategy" in result

    def test_enrich_catalog_entry(self):
        client = MockLLMClient('{"description": "Customer data", "business_purpose": "CRM", "quality_rules": []}')
        advisor = LLMAdvisor(client)
        result = advisor.enrich_catalog_entry({"dataset_name": "customers"}, {})
        assert "description" in result

    def test_review_transpiled_code(self):
        client = MockLLMClient('{"issues": [], "suggestions": [], "correctness_pct": 90}')
        advisor = LLMAdvisor(client)
        result = advisor.review_transpiled_code("DATA a; SET b; RUN;", "CREATE TABLE a AS SELECT * FROM b;", "Snowflake")
        assert "correctness_pct" in result

    def test_suggest_gap_resolution(self):
        client = MockLLMClient('{"suggestions": [{"gap": "PROC LOGISTIC", "resolution": "Use Snowpark ML", "effort": "HIGH"}]}')
        advisor = LLMAdvisor(client)
        result = advisor.suggest_gap_resolution({"gaps": ["PROC LOGISTIC"]})
        assert "suggestions" in result

    def test_handles_empty_response(self):
        client = MockLLMClient("")
        advisor = LLMAdvisor(client)
        result = advisor.validate_parser_output("code", {})
        assert result == {}

    def test_handles_json_in_markdown(self):
        client = MockLLMClient('```json\n{"findings": ["test"]}\n```')
        advisor = LLMAdvisor(client)
        result = advisor.validate_parser_output("code", {})
        assert result.get("findings") == ["test"]

    def test_handles_invalid_json(self):
        client = MockLLMClient("This is not JSON at all")
        advisor = LLMAdvisor(client)
        result = advisor.validate_parser_output("code", {})
        assert "raw_response" in result
