"""Demo workflow."""

from prefect import flow, get_run_logger

from cell_abm_pipeline.__config__ import ContextConfig, ParametersConfig, SeriesConfig


@flow(name="demo")
def run_flow(context: ContextConfig, series: SeriesConfig, parameters: ParametersConfig) -> None:
    """Main demo flow."""

    logger = get_run_logger()

    logger.info(context)
    logger.info(series)
    logger.info(parameters)
