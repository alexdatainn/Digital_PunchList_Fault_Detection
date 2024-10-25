from datetime import datetime
from typing import Dict, List

import pandas as pd
from digital_punchlist.bazefield_api import (AggregateType, BazefieldAPI,
                                             OutputItem)


def run(
    api: BazefieldAPI,
    site: str,
    turbines: List[str],
    start_time: datetime,
    end_time: datetime,
) -> List[OutputItem]:
    ret: List[OutputItem] = []
    occurances: Dict = {}
    values: Dict = {}
    interval_ms: int = 600000
    lower_filter: float = 50
    upper_filter: float = 900
    for asset in [site]:
        occurances[asset] = []
        values[asset] = []

    tags: List[str] = ["POIVoltage10mMAX", "POIVoltUpperLimit"]

    measures: pd.DataFrame = api.get_measurement_timeseries(
        [site], tags, start_time[0], end_time[0], interval_ms=interval_ms,
        aggregate_type=AggregateType.max)

    for asset in [site]:
        if (
            (('v_' + asset + '-' + tags[0]) in measures)
            and (('v_' + asset + '-' + tags[1]) in measures)
        ):
            for i, val in enumerate(measures['v_' + asset + '-' + tags[0]]):
                timestamp = measures['v_' + asset + '-' + tags[0]].index[i]
                volt_max = measures['v_' + asset + '-' + tags[0]][timestamp]
                limit = measures['v_' + asset + '-' + tags[1]][timestamp]
                if (
                    (volt_max > lower_filter) and (volt_max < upper_filter)
                    and (volt_max > limit)
                ):
                    occurances[asset].append({
                        "time": timestamp, "volt": volt_max, "limit": limit,
                    })

    for asset in occurances:
        if (len(occurances[asset]) > 0):
            ret.append({
                "asset": site + "-SUB",
                "priority": "Monitor",
                "certainty": "Monitor",
                "occurrence_start": start_time[0],
                "occurrence_end": end_time[0],
                "evidence": [{
                    "timestamp": api.datetime_to_unix_time(start_time[0]),
                    "variable": f"{site}-{tags[0]}",
                    "value": len(occurances[asset]),
                    "link":  api.generate_trend_link_from_evidence_using_start_and_end([site + '-' + tags[0], site + '-' + tags[1]], api.datetime_to_unix_time(start_time[0]), api.datetime_to_unix_time(end_time[0])),  # noqa: E501
                }],
                "estimated_occurrence_loss": 0,
                "estimated_aep_loss": 0,
                "estimated_life_reduction_days": 0,
                "status": "Office",
            })

    return ret
