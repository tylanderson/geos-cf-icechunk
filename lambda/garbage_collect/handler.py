import os
from datetime import datetime, timedelta, timezone

from aws_lambda_powertools import Logger
from virtualizarr_processor.processor import PROD_INIT_KEY, Processor

logger = Logger()


def handler() -> None:
    try:
        base_prefix = os.getenv("ICECHUNK_PREFIX", "")
        processor = Processor(
            prefix=base_prefix,
            init_key=PROD_INIT_KEY,
        )
        expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
        print(expiry_time)
        processor.garbage_collect(expiry_time=expiry_time)
        logger.info("Icechunk garbage collected")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
