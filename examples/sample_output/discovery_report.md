# SAS Environment Discovery Report

**Project:** MVP1 Mock Test
**Client:** MAPFRE
**Generated:** 2026-03-05 10:00:00

---

## 1. Executive Summary

| Metric | Value |
|--------|-------|
| Total Programs | 7 |
| Total Datasets | 5 |
| Lineage Nodes | 35 |
| Lineage Edges | 28 |
| Unique PROCs | 8 |
| Complexity Distribution | {'LOW': 3, 'MEDIUM': 3, 'HIGH': 1} |

**Recommended Strategy:** Phased migration recommended

## 2. Program Inventory

| Program | Lines | Complexity | PROCs | Datasets Read | Datasets Written |
|---------|-------|------------|-------|---------------|------------------|
| risk_model_scoring.sas | 45 | HIGH (28) | LOGISTIC, UNIVARIATE, FREQ, MEANS | 3 | 4 |
| etl_claims_processing.sas | 30 | MEDIUM (18) | MEANS, FREQ | 2 | 1 |
| etl_load_customers.sas | 55 | MEDIUM (22) | SORT, SQL, FREQ, MEANS | 4 | 3 |
| etl_load_policies.sas | 32 | MEDIUM (14) | SQL, SORT | 3 | 2 |
| etl_dynamic_loader.sas | 28 | LOW (10) | SQL | 1 | 1 |
| report_monthly_kpi.sas | 25 | LOW (6) | SQL, TABULATE, REPORT | 1 | 1 |
| log_utils.sas | 20 | LOW (8) | SQL | 0 | 0 |

## 3. Dataset Inventory

| Dataset | Rows | Columns | Size |
|---------|------|---------|------|
| policies | 2000 | 7 | 156.3 KB |
| customers_raw | 1000 | 16 | 250.0 KB |
| addresses | 800 | 5 | 62.5 KB |
| claims | 500 | 6 | 46.9 KB |
| products | 5 | 3 | 1.0 KB |

## 4. Dependencies and Lineage

| Metric | Value |
|--------|-------|
| Total Nodes | 35 |
| Total Edges | 28 |
| Node Types | {'program': 7, 'dataset': 22, 'macro': 4, 'include': 2} |
| Root Nodes | 6 |
| Leaf Nodes | 8 |

## 5. Complexity Analysis

### Top 20 Most Complex Programs

| Rank | Program | Score | Level | Key Factors |
|------|---------|-------|-------|-------------|
| 1 | risk_model_scoring.sas | 28 | HIGH | Macros (1), LOGISTIC, UNIVARIATE |
| 2 | etl_load_customers.sas | 22 | MEDIUM | MERGE (2 tables), Macros (1) |
| 3 | etl_claims_processing.sas | 18 | MEDIUM | Hash Objects |
| 4 | etl_load_policies.sas | 14 | MEDIUM | MERGE (2 tables) |
| 5 | etl_dynamic_loader.sas | 10 | LOW | Dynamic SQL |
| 6 | log_utils.sas | 8 | LOW | Dynamic SQL |
| 7 | report_monthly_kpi.sas | 6 | LOW | Standard |

## 6. Limitations and Notes

- SAS code parser is regex-based, not a full AST parser. Coverage ~80% of common patterns.
- Dataset metadata extracted from .meta.json fallback files.
- PROC LOGISTIC, REG, GLM, MIXED have no direct Snowflake SQL equivalent.
- Hash objects and CALL EXECUTE require manual review.
- Lineage is inferred from code; runtime dependencies may differ.

## 7. Next Steps

1. Review complexity analysis and prioritize programs for migration.
2. Validate lineage graph with business stakeholders.
3. Run MVP2 to generate Snowflake DDL, COPY INTO, and transpiled code.
4. Enable LLM review for architecture recommendations.
5. Set up Snowflake staging environment and test data loads.
6. Plan UAT with business users.
