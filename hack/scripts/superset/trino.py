from datetime import datetime
from typing import Optional

from superset.db_engine_specs import BaseEngineSpec
from superset.utils import core as utils


class TrinoEngineSpec(BaseEngineSpec):
    engine = "trino"
    engine_name = "Trino"

    # pylint: disable=line-too-long
    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "date_trunc('second', CAST({col} AS TIMESTAMP))",
        "PT1M": "date_trunc('minute', CAST({col} AS TIMESTAMP))",
        "PT1H": "date_trunc('hour', CAST({col} AS TIMESTAMP))",
        "P1D": "date_trunc('day', CAST({col} AS TIMESTAMP))",
        "P1W": "date_trunc('week', CAST({col} AS TIMESTAMP))",
        "P1M": "date_trunc('month', CAST({col} AS TIMESTAMP))",
        "P0.25Y": "date_trunc('quarter', CAST({col} AS TIMESTAMP))",
        "P1Y": "date_trunc('year', CAST({col} AS TIMESTAMP))",
        # "1969-12-28T00:00:00Z/P1W",  # Week starting Sunday
        # "1969-12-29T00:00:00Z/P1W",  # Week starting Monday
        # "P1W/1970-01-03T00:00:00Z",  # Week ending Saturday
        # "P1W/1970-01-04T00:00:00Z",  # Week ending Sunday
    }

    @classmethod
    def convert_dttm(cls, target_type: str, dttm: datetime) -> Optional[str]:
        tt = target_type.upper()
        if tt == utils.TemporalType.DATE:
            value = dttm.date().isoformat()
            return f"from_iso8601_date('{value}')"
        if tt == utils.TemporalType.TIMESTAMP:
            value = dttm.isoformat(timespec='microseconds')
            return f"from_iso8601_timestamp('{value}')"
        return None

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "from_unixtime({col})"
