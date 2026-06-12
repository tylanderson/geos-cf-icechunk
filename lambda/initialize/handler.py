import os
from typing import Any

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor.processor import (
    PROD_END_DT,
    PROD_INIT_KEY,
    PROD_START_DT,
    Processor,
)

logger = Logger()
tracer = Tracer()


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> None:
    try:
        base_prefix = os.getenv("ICECHUNK_PREFIX", "")
        processor = Processor(
            prefix=base_prefix,
            init_key=PROD_INIT_KEY,
            init_dt_range=(PROD_START_DT, PROD_END_DT),
        )
        _ = processor.initialize_store()
        logger.info("Icechunk initialized")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
