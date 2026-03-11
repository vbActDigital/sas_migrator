"""Unit tests for Lineage Builder."""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from src.parsers.sas.lineage_builder import LineageBuilder


@pytest.fixture
def builder():
    return LineageBuilder()


@pytest.fixture
def sample_parsed_programs():
    return [
        {
            "filename": "etl_load.sas",
            "datasets_read": ["rawdata.customers", "rawdata.addresses"],
            "datasets_written": ["work.customers_clean", "dw.dim_customer"],
            "macro_definitions": ["log_step"],
            "macro_calls": ["log_step"],
            "includes": [],
        },
        {
            "filename": "etl_policies.sas",
            "datasets_read": ["rawdata.policies", "dw.dim_customer"],
            "datasets_written": ["dw.fact_policies"],
            "macro_definitions": [],
            "macro_calls": ["log_step"],
            "includes": [],
        },
        {
            "filename": "report.sas",
            "datasets_read": ["dw.fact_policies", "dw.dim_customer"],
            "datasets_written": ["work.monthly_report"],
            "macro_definitions": [],
            "macro_calls": [],
            "includes": ["/macros/utils.sas"],
        },
    ]


class TestLineageBuilder:
    def test_build_returns_dict(self, builder, sample_parsed_programs):
        result = builder.build_from_parsed_programs(sample_parsed_programs)
        assert "nodes" in result
        assert "edges" in result

    def test_nodes_have_required_fields(self, builder, sample_parsed_programs):
        result = builder.build_from_parsed_programs(sample_parsed_programs)
        for node in result["nodes"]:
            assert "id" in node
            assert "type" in node
            assert "label" in node

    def test_edges_have_required_fields(self, builder, sample_parsed_programs):
        result = builder.build_from_parsed_programs(sample_parsed_programs)
        for edge in result["edges"]:
            assert "source" in edge
            assert "target" in edge
            assert "relationship" in edge

    def test_node_types(self, builder, sample_parsed_programs):
        result = builder.build_from_parsed_programs(sample_parsed_programs)
        types = {n["type"] for n in result["nodes"]}
        assert "program" in types
        assert "dataset" in types

    def test_correct_node_count(self, builder, sample_parsed_programs):
        result = builder.build_from_parsed_programs(sample_parsed_programs)
        assert len(result["nodes"]) >= 10

    def test_correct_edge_count(self, builder, sample_parsed_programs):
        result = builder.build_from_parsed_programs(sample_parsed_programs)
        assert len(result["edges"]) >= 10

    def test_get_upstream(self, builder, sample_parsed_programs):
        builder.build_from_parsed_programs(sample_parsed_programs)
        upstream = builder.get_upstream("program:report.sas")
        assert len(upstream) > 0

    def test_get_downstream(self, builder, sample_parsed_programs):
        builder.build_from_parsed_programs(sample_parsed_programs)
        downstream = builder.get_downstream("program:etl_load.sas")
        assert len(downstream) > 0

    def test_dim_customer_connects_programs(self, builder, sample_parsed_programs):
        builder.build_from_parsed_programs(sample_parsed_programs)
        downstream = builder.get_downstream("dataset:dw.dim_customer")
        assert len(downstream) > 0

    def test_empty_input(self, builder):
        result = builder.build_from_parsed_programs([])
        assert result["nodes"] == []
        assert result["edges"] == []

    def test_single_program(self, builder):
        result = builder.build_from_parsed_programs([{
            "filename": "solo.sas",
            "datasets_read": ["input"],
            "datasets_written": ["output"],
            "macro_definitions": [],
            "macro_calls": [],
            "includes": [],
        }])
        assert len(result["nodes"]) == 3  # program + 2 datasets
        assert len(result["edges"]) == 2  # reads + writes

    def test_include_nodes(self, builder, sample_parsed_programs):
        result = builder.build_from_parsed_programs(sample_parsed_programs)
        include_nodes = [n for n in result["nodes"] if n["type"] == "include"]
        assert len(include_nodes) == 1
