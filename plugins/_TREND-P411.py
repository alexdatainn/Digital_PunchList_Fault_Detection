from datetime import datetime, timedelta
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
    for turb in turbines:
        occurances[turb] = []
    interval_ms: int = 6*600000
    min_temp: int = -60
    max_temp: int = 60
    max_diff: int = 3
    min_duration: float = 0.75*(end_time[0]-start_time[0])/timedelta(hours=1)
    # incase of gamesa it shows mixcalibration of the sensor, if turbine
    tags: List[str] = ["AmbientTemperature"]
    measures: pd.DataFrame = api.get_measurement_timeseries(
        turbines, tags, start_time[0], end_time[0], interval_ms,
        aggregate_type=AggregateType.average
        )

    for tag in tags:
        full_tags: List[str] = []
        for turb in turbines:
            if ('v_' + turb + '-' + tag) in measures:
                full_tags.append('v_' + turb + '-' + tag)

        median_series = pd.DataFrame(measures[full_tags].median(axis='columns'))  # noqa E501
        median_series.columns = ['median_' + tag]
        measures = measures.join(median_series)

        for turb in turbines:
            if ('v_' + turb + '-' + tag) in measures:
                for ind, val in enumerate(measures['v_' + turb + '-' + tag]):
                    timestamp = measures['median_' + tag].index[ind]
                    median_val = measures['median_' + tag][timestamp]
                    if (
                        (abs(median_val-val) > max_diff)
                        and (median_val < max_temp) and (median_val > min_temp)
                        and (val < max_temp) and (val > min_temp)
                    ):
                        occurances[turb].append({
                            "timestamp": timestamp,
                            "value": val,
                            "referenceValue": median_val
                            })

    for asset in occurances:
        if len(occurances[asset]) > min_duration:
            ret.append(
                {
                    "asset": asset,
                    "priority": "Next PM",
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "evidence": [{
                        "timestamp": int(occurances[asset][0]["timestamp"]),
                        "variable": f"{asset}-{tags[0]}",
                        "value": occurances[asset][0]["value"],
                        "link": api.generate_trend_link_from_evidence_using_padding(  # noqa E501
                            [asset + '-' + tags[0]],
                            occurances[asset][0]["timestamp"], interval_ms
                        ),
                    }],
                    "estimated_occurrence_loss": 0,
                    "estimated_aep_loss": 72,
                    "estimated_life_reduction_days": 0,
                    "status": "Office",
                }
            )

    return ret
