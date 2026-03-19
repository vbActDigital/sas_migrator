from datetime import datetime
from typing import Dict, List, Optional

from src.utils.logger import get_logger

logger = get_logger("catalog_generator")

PII_KEYWORDS = {"cpf", "email", "phone", "telefone", "salary", "salario", "ssn", "rg",
                "nome", "name", "address", "endereco"}

DOMAIN_KEYWORDS = {
    "Customer": ["customer", "client", "pessoa", "cadastro"],
    "Financial": ["payment", "transaction", "premium", "amount", "financial", "revenue"],
    "Risk": ["risk", "score", "model", "scoring"],
    "Product": ["product", "produto", "item"],
    "Claims": ["claim", "sinistro"],
    "Policy": ["policy", "apolice", "contract"],
}


class DataCatalogGenerator:
    def __init__(self, config: Dict, llm_advisor=None):
        self.config = config
        self.llm_advisor = llm_advisor
        catalog_config = config.get("catalog", {})
        self.detect_pii = catalog_config.get("detect_pii", True)
        self.infer_domains = catalog_config.get("infer_domains", True)

    def generate_catalog(self, datasets_metadata: List[Dict],
                         programs_metadata: Optional[List[Dict]] = None,
                         lineage_data: Optional[Dict] = None,
                         enrich_with_llm: bool = True) -> Dict:
        catalog_datasets = []
        all_columns = []
        domains_count: Dict[str, int] = {}
        sensitivity_count = {"Restricted": 0, "Internal": 0, "Public": 0}
        datasets_with_pii = 0

        for ds_meta in datasets_metadata:
            columns = ds_meta.get("columns", [])
            pii_columns = self._detect_pii_columns(columns) if self.detect_pii else []
            domain = self._infer_domain(ds_meta) if self.infer_domains else "Unknown"
            has_pii = len(pii_columns) > 0

            if has_pii:
                sensitivity = "Restricted"
                datasets_with_pii += 1
            elif self._is_reference_table(ds_meta):
                sensitivity = "Public"
            else:
                sensitivity = "Internal"

            sensitivity_count[sensitivity] += 1
            domains_count[domain] = domains_count.get(domain, 0) + 1

            ds_entry = {
                "dataset_name": ds_meta.get("dataset_name", ds_meta.get("filename", "")),
                "row_count": ds_meta.get("row_count", -1),
                "column_count": ds_meta.get("column_count", len(columns)),
                "domain": domain,
                "sensitivity": sensitivity,
                "pii_columns": pii_columns,
                "columns": columns,
            }

            if enrich_with_llm and self.llm_advisor:
                try:
                    enrichment = self.llm_advisor.enrich_catalog_entry(ds_meta, {})
                    ds_entry["llm_description"] = enrichment.get("description", "")
                    ds_entry["llm_quality_rules"] = enrichment.get("quality_rules", [])
                except Exception as e:
                    logger.warning("LLM enrichment failed for %s: %s", ds_entry["dataset_name"], e)

            catalog_datasets.append(ds_entry)

            for col in columns:
                col_entry = {
                    "dataset": ds_entry["dataset_name"],
                    "column_name": col.get("name", ""),
                    "type": col.get("type", ""),
                    "is_pii": col.get("name", "").lower() in PII_KEYWORDS,
                }
                all_columns.append(col_entry)

        total_columns = sum(ds.get("column_count", 0) for ds in catalog_datasets)

        catalog = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "generator": "DataCatalogGenerator",
                "total_datasets": len(catalog_datasets),
                "total_columns": total_columns,
            },
            "summary": {
                "total_datasets": len(catalog_datasets),
                "total_columns": total_columns,
                "domains": domains_count,
                "sensitivity_distribution": sensitivity_count,
                "datasets_with_pii": datasets_with_pii,
            },
            "datasets": catalog_datasets,
            "columns": all_columns,
        }

        logger.info("Catalog generated: %d datasets, %d columns", len(catalog_datasets), total_columns)
        return catalog

    def _detect_pii_columns(self, columns: List[Dict]) -> List[str]:
        pii = []
        for col in columns:
            col_name = col.get("name", "").lower()
            if col_name in PII_KEYWORDS or any(kw in col_name for kw in PII_KEYWORDS):
                pii.append(col.get("name", ""))
        return pii

    def _infer_domain(self, ds_meta: Dict) -> str:
        ds_name = ds_meta.get("dataset_name", "").lower()
        columns_str = " ".join(c.get("name", "") for c in ds_meta.get("columns", [])).lower()
        search_text = f"{ds_name} {columns_str}"

        for domain, keywords in DOMAIN_KEYWORDS.items():
            for kw in keywords:
                if kw in search_text:
                    return domain
        return "General"

    def _is_reference_table(self, ds_meta: Dict) -> bool:
        row_count = ds_meta.get("row_count", -1)
        ds_name = ds_meta.get("dataset_name", "").lower()
        if row_count > 0 and row_count <= 100:
            return True
        if any(kw in ds_name for kw in ("lookup", "ref", "product", "code", "type")):
            return True
        return False
