from datetime import datetime, timedelta, timezone

import icechunk
from icechunk import Repository
from virtualizarr_processor.processor import PROD_INIT_KEY, Processor
from virtualizarr_processor.typing import VirtualizarrProcessor


def protocol_type_check(processor: VirtualizarrProcessor) -> None:
    assert processor


def test_follows_protocol() -> None:
    processor = Processor()
    protocol_type_check(processor=processor)


def test_initialize_store() -> None:
    processor = Processor(init_key=PROD_INIT_KEY)
    repo = processor.initialize_store()
    assert isinstance(repo, Repository)


def test_process_file() -> None:
    processor = Processor(init_key=PROD_INIT_KEY)
    snapshot = processor.process_file(
        file_keys=[
            "GMAO/GEOS-CF/analysis-v2/Y2025/M08/D04/GEOS.cf.ana.aqc_tavg_1hr_glo_L1440x721_slv.20250804_0930z.nc4"
        ]
    )
    assert isinstance(snapshot, str)


def test_garbage_collect() -> None:
    processor = Processor(init_key=PROD_INIT_KEY)
    expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
    gcs = processor.garbage_collect(expiry_time=expiry_time)
    assert isinstance(gcs, icechunk.GCSummary)
