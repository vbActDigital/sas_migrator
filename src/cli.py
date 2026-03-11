import click

from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger

logger = get_logger("cli")


@click.group()
def cli():
    """SAS-to-Snowflake Migration Toolkit"""
    pass


@cli.command()
@click.option("--config", required=True, help="Path to YAML config file")
@click.option("--out", required=True, help="Output directory")
@click.option("--catalog", is_flag=True, help="Generate data catalog")
@click.option("--llm-validate", is_flag=True, help="Use LLM to validate parser output")
@click.option("--llm-architecture", is_flag=True, help="Use LLM for architecture review")
def discover(config, out, catalog, llm_validate, llm_architecture):
    """Run MVP1 Discovery - analyze SAS environment"""
    loader = ConfigLoader(config)
    cfg = loader.load()

    llm_advisor = None
    if llm_validate or llm_architecture:
        try:
            from src.llm.llm_client import LLMClient
            from src.llm.llm_advisor import LLMAdvisor
            client = LLMClient(cfg)
            if client.is_available:
                llm_advisor = LLMAdvisor(client)
            else:
                logger.warning("LLM requested but no API key available")
        except Exception as e:
            logger.warning("Failed to initialize LLM: %s", e)

    from src.services.discovery_service import DiscoveryService
    service = DiscoveryService(cfg, llm_advisor)
    results = service.run(out, catalog=catalog, llm_validate=llm_validate,
                          llm_architecture=llm_architecture)
    click.echo(f"Discovery complete. Report: {results.get('report', 'N/A')}")


@cli.command()
@click.option("--inventory", required=True, help="Path to inventory JSON from discovery")
@click.option("--config", required=True, help="Path to YAML config file")
@click.option("--out", required=True, help="Output directory")
@click.option("--llm-review", is_flag=True, help="Use LLM to review transpiled code")
@click.option("--llm-gaps", is_flag=True, help="Use LLM for gap resolution suggestions")
@click.option("--validate-only", is_flag=True, help="Only generate validation scripts")
def migrate(inventory, config, out, llm_review, llm_gaps, validate_only):
    """Run MVP2 Migration - generate Snowflake artifacts"""
    loader = ConfigLoader(config)
    cfg = loader.load()

    llm_advisor = None
    if llm_review or llm_gaps:
        try:
            from src.llm.llm_client import LLMClient
            from src.llm.llm_advisor import LLMAdvisor
            client = LLMClient(cfg)
            if client.is_available:
                llm_advisor = LLMAdvisor(client)
        except Exception as e:
            logger.warning("Failed to initialize LLM: %s", e)

    from src.services.migration_service import MigrationService
    service = MigrationService(cfg, llm_advisor)
    results = service.run(inventory, out, llm_review=llm_review,
                          llm_gaps=llm_gaps, validate_only=validate_only)
    click.echo(f"Migration complete. Output: {out}")


if __name__ == "__main__":
    cli()
