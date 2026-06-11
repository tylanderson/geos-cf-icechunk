import json
import os
from typing import Any, Dict
from urllib.parse import unquote_plus

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.batch.types import PartialItemFailureResponse
from aws_lambda_powertools.utilities.data_classes import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from virtualizarr_processor.processor import (
    PROD_END_DT,
    PROD_INIT_KEY,
    PROD_START_DT,
    THREDDS_URL,
    Processor,
)

logger = Logger()
tracer = Tracer()
processor = BatchProcessor(event_type=EventType.SQS)

BASE_PREFIX = os.getenv("ICECHUNK_PREFIX", "")
PROCESSOR = Processor(
    prefix=BASE_PREFIX,
    init_key=PROD_INIT_KEY,
    init_dt_range=(PROD_START_DT, PROD_END_DT),
)


def _normalize_to_processor_key(key: str) -> str:
    normalized_key = unquote_plus(key).lstrip("/")
    normalized_thredds = THREDDS_URL.rstrip("/")

    if normalized_key.startswith(f"{normalized_thredds}/"):
        return normalized_key.removeprefix(f"{normalized_thredds}/")

    if normalized_key.startswith("http://") or normalized_key.startswith("https://"):
        return normalized_key.split("/thredds/fileServer/", maxsplit=1)[-1]

    return normalized_key


@tracer.capture_method
def record_handler(record: SQSRecord) -> None:
    """
    Process individual SQS record.

    Args:
        record: SQS record from the batch
    """
    try:
        # Extract message body
        message_body = record.body

        # Parse the SNS message if it's from SNS
        message = json.loads(message_body)

        # If message is from SNS, extract the actual message
        if "Message" in message:
            sns_message = json.loads(message["Message"])
            process_notification(sns_message)
        else:
            # Direct SQS message
            process_notification(message)

    except Exception as e:
        logger.error(
            f"Error processing record: {str(e)}",
            extra={"message_id": record.message_id},
        )
        raise


@tracer.capture_method
def process_notification(message: Dict[str, Any]) -> None:
    """
    Process a notification message.

    Args:
        message: The notification message to process.
            Supports ``"overwrite": true`` to use region writes
            instead of the default append.
    """
    overwrite = bool(message.get("overwrite", False))

    # Extract file URL from the message
    file_url = message.get("url") or message.get("file_url") or message.get("http_url")
    file_urls = message.get("urls", [])
    file_keys: list[str] = []
    if file_url:
        file_key = _normalize_to_processor_key(file_url)
        http_url = f"{THREDDS_URL.rstrip('/')}/{file_key}"
        logger.info(
            "Append file(s)",
            extra={
                "input_url": file_url,
                "file_key": file_key,
                "http_url": http_url,
                "overwrite": overwrite,
            },
        )
        file_keys = [file_key]
        logger.info(f"{http_url} successfully processed")
    elif file_urls:
        file_keys = [_normalize_to_processor_key(url) for url in file_urls]
        logger.info(
            "Append files",
            extra={
                "input_urls": file_urls,
                "file_keys": file_keys,
                "overwrite": overwrite,
            },
        )
        logger.info(f"{len(file_keys)} files successfully processed")
    else:
        raise ValueError("No valid file URL found in message")

    logger.info(f"Processing {len(file_keys)} files")
    _ = PROCESSOR.process_file(file_keys=file_keys, overwrite=overwrite)


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> PartialItemFailureResponse:
    """
    Lambda function to process notification messages from SQS queue.

    Args:
        event: Lambda event containing SQS records
        context: Lambda context object

    """
    return process_partial_response(
        event=event,
        record_handler=record_handler,
        processor=processor,
        context=context,
    )
