import time
from datetime import timezone

from pipeline_common.timing import elapsed, now_utc


def test_now_utc_has_whole_seconds() -> None:
    now = now_utc()
    assert now.tzinfo == timezone.utc
    assert now.microsecond == 0


def test_elapsed_is_rounded() -> None:
    seconds = elapsed(time.monotonic() - 1.23456)
    assert seconds == round(seconds, 2)
    assert seconds >= 1.23
