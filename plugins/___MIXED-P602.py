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
    interval_ms: int = 24*6*600000
    low_volt: float = 50
    upper_volt: float = 500
    volt_delta_limit: float = 0.001
    for asset in [site]:
        occurances[asset] = []
        values[asset] = []

    tags: List[str] = ["POIVoltage10mMAX", "POIVoltage10mMIN"]  # noqa E501

    measures: pd.DataFrame = api.get_measurement_timeseries(
        [site], tags, start_time[0], end_time[0], interval_ms=interval_ms,
        aggregate_type=AggregateType.time_average)

    for asset in [site]:
        if (
            (('v_' + asset + '-' + tags[0]) in measures)
            and (('v_' + asset + '-' + tags[1]) in measures)
        ):
            for i, val in enumerate(measures['v_' + asset + '-' + tags[0]]):
                timestamp = measures['v_' + asset + '-' + tags[0]].index[i]
                max_volt = measures['v_' + asset + '-' + tags[0]][timestamp]
                min_volt = measures['v_' + asset + '-' + tags[1]][timestamp]
                if (
                    (max_volt > low_volt) and (min_volt > low_volt)
                    and (max_volt < upper_volt) and (min_volt < upper_volt)
                    and ((max_volt-min_volt)/max_volt > volt_delta_limit)
                ):
                    occurances[asset].append({
                        "timestamp": timestamp,
                        "max_volt": max_volt,
                        "min_volt": min_volt,
                        "diff_percent": (max_volt-min_volt)/max_volt,
                        })

    for occ in occurances[site]:
        ret.append(
            {
                "asset": site,
                "priority": "ASAP",
                "certainty": "Auto-Escalate",
                "occurrence_start": start_time[0],
                "occurrence_end": end_time[0],
                "evidence": [{
                    "timestamp": int(occ["timestamp"]),
                    "variable": f"{site}-{tags[0]}",
                    "value": 0,
                    "link":  api.generate_trend_link_from_evidence_using_padding([site + '-' + tags[0], site + '-' + tags[1]], occ["timestamp"], interval_ms),  # noqa: E501
                }],
                "estimated_occurrence_loss": len(occ)*0.1,
                "estimated_aep_loss": 17*len(occ),
                "estimated_life_reduction_days": 0,
                "status": "Office",
            }
        )

    return ret
