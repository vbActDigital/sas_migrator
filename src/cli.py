import os
import click

from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger("cli")


@click.group()
def cli():
    """SAS Migration Toolkit - Analise e migracao de ambientes SAS."""
    pass


@cli.command()
@click.option("--sas-path", required=False, help="Path to SAS programs/scripts folder (overrides config)")
@click.option("--data-path", required=False, help="Path to SAS datasets folder (overrides config)")
@click.option("--config", required=False, help="Path to YAML config file (optional if --sas-path is given)")
@click.option("--out", required=True, help="Output directory for all artifacts")
@click.option("--target", type=click.Choice(["snowflake", "databricks"]), default="snowflake",
              help="Target platform (default: snowflake)")
@click.option("--catalog", is_flag=True, default=True, help="Generate data catalog (default: enabled)")
@click.option("--no-catalog", is_flag=True, help="Disable data catalog generation")
@click.option("--pdf", is_flag=True, default=True, help="Generate PDF report (default: enabled)")
@click.option("--no-pdf", is_flag=True, help="Disable PDF report generation")
@click.option("--llm-validate", is_flag=True, help="Use LLM to validate parser output")
@click.option("--llm-architecture", is_flag=True, help="Use LLM for architecture review")
def discover(sas_path, data_path, config, out, target, catalog, no_catalog, pdf, no_pdf,
             llm_validate, llm_architecture):
    """Analyze a SAS environment: scan, parse, catalog, report.

    \b
    Examples:
      # Point to a folder with SAS scripts
      sas-migrator discover --sas-path /path/to/sas/programs --out output

      # Point to both programs and datasets
      sas-migrator discover --sas-path /sas/programs --data-path /sas/data --out output

      # Use a full config file
      sas-migrator discover --config config/snowflake_aws_config.yaml --out output

      # Target Databricks instead of Snowflake
      sas-migrator discover --sas-path /sas/programs --out output --target databricks
    """
    cfg = _build_config(config, sas_path, data_path, target)
    generate_catalog = catalog and not no_catalog
    generate_pdf = pdf and not no_pdf

    llm_advisor = _init_llm(cfg, llm_validate or llm_architecture)

    from src.services.discovery_service import DiscoveryService
    service = DiscoveryService(cfg, llm_advisor)
    results = service.run(
        out, catalog=generate_catalog, pdf=generate_pdf,
        llm_validate=llm_validate,
        llm_architecture=llm_architecture,
    )

    click.echo(f"\nDiscovery completo.")
    click.echo(f"  Relatorio MD:  {results.get('report', 'N/A')}")
    if results.get("pdf_report"):
        click.echo(f"  Relatorio PDF: {results['pdf_report']}")
    if results.get("catalog"):
        click.echo(f"  Catalogo:      {results['catalog']}")
    click.echo(f"  Inventario:    {results.get('inventory', 'N/A')}")

    # Show manual intervention summary
    if results.get("manual_review"):
        click.echo(f"\n  ATENCAO: {len(results['manual_review'])} item(s) requerem intervencao manual.")
        for item in results["manual_review"][:10]:
            click.echo(f"    - {item}")


@cli.command()
@click.option("--inventory", required=True, help="Path to inventory.json from discover step")
@click.option("--config", required=False, help="Path to YAML config file")
@click.option("--sas-path", required=False, help="Path to SAS programs folder")
@click.option("--out", required=True, help="Output directory for migration artifacts")
@click.option("--target", type=click.Choice(["snowflake", "databricks"]), default="snowflake",
              help="Target platform (default: snowflake)")
@click.option("--llm-review", is_flag=True, help="Use LLM to review transpiled code")
@click.option("--llm-gaps", is_flag=True, help="Use LLM for gap resolution suggestions")
@click.option("--validate-only", is_flag=True, help="Only generate validation scripts")
def migrate(inventory, config, sas_path, out, target, llm_review, llm_gaps, validate_only):
    """Generate migration artifacts: DDL, data load, transpiled code.

    \b
    Examples:
      # Snowflake migration
      sas-migrator migrate --inventory output/inventory.json --out output/migration

      # Databricks migration
      sas-migrator migrate --inventory output/inventory.json --out output/migration --target databricks

      # With LLM gap analysis
      sas-migrator migrate --inventory output/inventory.json --out output/migration --llm-gaps
    """
    cfg = _build_config(config, sas_path, None, target)

    llm_advisor = _init_llm(cfg, llm_review or llm_gaps)

    from src.services.migration_service import MigrationService
    service = MigrationService(cfg, llm_advisor)
    results = service.run(
        inventory, out, llm_review=llm_review,
        llm_gaps=llm_gaps, validate_only=validate_only,
    )

    click.echo(f"\nMigracao completa. Artefatos em: {out}")
    if results.get("manual_interventions"):
        click.echo(f"\n  INTERVENCAO MANUAL necessaria:")
        for item in results["manual_interventions"]:
            click.echo(f"    [{item['severity']}] {item['program']}: {item['reason']}")


@cli.command()
@click.option("--sas-path", required=True, help="Path to SAS programs/scripts folder")
@click.option("--data-path", required=False, help="Path to SAS datasets folder")
@click.option("--out", required=True, help="Output directory for all artifacts")
@click.option("--target", type=click.Choice(["snowflake", "databricks"]), default="snowflake",
              help="Target platform")
@click.option("--config", required=False, help="Path to YAML config file (optional)")
@click.option("--llm", is_flag=True, help="Enable LLM for all analysis steps")
def run(sas_path, data_path, out, target, config, llm):
    """Run full pipeline: discover + migrate in one step.

    \b
    Examples:
      sas-migrator run --sas-path /path/to/sas --out output --target snowflake
      sas-migrator run --sas-path /sas/programs --data-path /sas/data --out output --llm
    """
    cfg = _build_config(config, sas_path, data_path, target)
    llm_advisor = _init_llm(cfg, llm)

    # Step 1: Discovery
    click.echo("=" * 60)
    click.echo("  FASE 1: DISCOVERY (analise do ambiente SAS)")
    click.echo("=" * 60)

    from src.services.discovery_service import DiscoveryService
    discovery = DiscoveryService(cfg, llm_advisor)
    disc_results = discovery.run(
        out, catalog=True, pdf=True,
        llm_validate=llm, llm_architecture=llm,
    )

    click.echo(f"\n  Relatorio: {disc_results.get('report', 'N/A')}")
    if disc_results.get("pdf_report"):
        click.echo(f"  PDF: {disc_results['pdf_report']}")

    # Step 2: Migration
    click.echo("\n" + "=" * 60)
    click.echo("  FASE 2: MIGRACAO (de-para e conversao)")
    click.echo("=" * 60)

    migration_out = os.path.join(out, "migration")
    from src.services.migration_service import MigrationService
    migration = MigrationService(cfg, llm_advisor)
    mig_results = migration.run(
        disc_results["inventory"], migration_out,
        llm_review=llm, llm_gaps=llm,
    )

    # Final summary
    click.echo("\n" + "=" * 60)
    click.echo("  RESUMO FINAL")
    click.echo("=" * 60)
    click.echo(f"  Plataforma alvo: {target}")
    click.echo(f"  Artefatos em:    {out}")

    if mig_results.get("manual_interventions"):
        click.echo(f"\n  INTERVENCAO MANUAL necessaria ({len(mig_results['manual_interventions'])} itens):")
        for item in mig_results["manual_interventions"]:
            click.echo(f"    [{item['severity']}] {item['program']}: {item['reason']}")
    else:
        click.echo(f"\n  Todos os programas foram convertidos automaticamente.")


def _build_config(config_path, sas_path, data_path, target):
    """Build configuration from file and/or CLI arguments."""
    if config_path:
        loader = ConfigLoader(config_path)
        cfg = loader.load()
    else:
        cfg = {
            "project": {"name": "SAS Migration", "client": ""},
            "sas_environment": {
                "code_paths": [],
                "data_paths": [],
                "log_paths": [],
                "exclude_patterns": ["backup", "archive"],
            },
            "target": {"platform": target or "snowflake"},
            "library_mapping": {},
            "catalog": {"detect_pii": True, "infer_domains": True},
        }

    # CLI overrides
    if sas_path:
        sas_path = os.path.abspath(sas_path)
        cfg.setdefault("sas_environment", {})
        cfg["sas_environment"]["code_paths"] = [sas_path]
        # If sas_path has subdirs like macros, include them
        macros_dir = os.path.join(sas_path, "macros")
        if os.path.isdir(macros_dir):
            cfg["sas_environment"]["code_paths"].append(macros_dir)

    if data_path:
        data_path = os.path.abspath(data_path)
        cfg.setdefault("sas_environment", {})
        cfg["sas_environment"]["data_paths"] = [data_path]

    if target:
        cfg.setdefault("target", {})
        cfg["target"]["platform"] = target

    # Default library mapping if not set
    if not cfg.get("library_mapping"):
        if target == "databricks":
            cfg["library_mapping"] = {
                "rawdata": {"catalog": "sas_migration", "schema": "bronze_sas"},
                "dw": {"catalog": "sas_migration", "schema": "silver_sas"},
            }
        else:
            cfg["library_mapping"] = {
                "rawdata": {"database": "SAS_MIGRATION", "schema": "RAW"},
                "dw": {"database": "SAS_MIGRATION", "schema": "REFINED"},
            }

    return cfg


def _init_llm(cfg, needed):
    if not needed:
        return None
    try:
        from src.llm.llm_client import LLMClient
        from src.llm.llm_advisor import LLMAdvisor
        client = LLMClient(cfg)
        if client.is_available:
            return LLMAdvisor(client)
        else:
            logger.warning("LLM solicitado mas OPENAI_API_KEY nao definida")
    except Exception as e:
        logger.warning("Falha ao inicializar LLM: %s", e)
    return None


if __name__ == "__main__":
    cli()
