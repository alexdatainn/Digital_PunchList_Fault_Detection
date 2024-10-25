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
    max_derating: float = 0.10
    min_energy: float = 0.2
    for turb in turbines:
        occurances[turb] = []
        values[turb] = []

    tags: List[str] = ["PI-ProducedMWh.AD.SUM", "PI-TheoreticalProdMWh.AD.SUM", "IEC-OperationState"]  # noqa E501

    measures: pd.DataFrame = api.get_measurement_timeseries(
        turbines, tags, start_time[0], end_time[0], interval_ms=interval_ms,
        aggregate_type=AggregateType.time_average
        )

    for turb in turbines:
        if (
            (('v_' + turb + '-' + tags[0]) in measures)
            and (('v_' + turb + '-' + tags[1]) in measures)
            and (('v_' + turb + '-' + tags[2]) in measures)
        ):
            for i, val in enumerate(measures['v_' + turb + '-' + tags[0]]):
                timestamp = measures['v_' + turb + '-' + tags[0]].index[i]
                act_value = measures['v_' + turb + '-' + tags[0]][timestamp]
                theo_value = measures['v_' + turb + '-' + tags[1]][timestamp]
                state_value = measures['v_' + turb + '-' + tags[2]][timestamp]
                # print(timestamp, act_value, theo_value, state_value)
                if (
                    (state_value == 4)
                    and (act_value > min_energy)
                    and ((theo_value - act_value)/theo_value > max_derating)
                ):
                    occurances[turb].append(timestamp)

    for asset in occurances:
        if len(occurances[asset]) > 0:
            ret.append(
                {
                    "asset": asset,
                    "priority": "ASAP",
                    "certainty": "Auto-Escalate",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "evidence": [{
                        "timestamp": int(occurances[asset][0]),
                        "variable": f"{asset}-{tags[0]}",
                        "value": 0,
                        "link": api.generate_trend_link_from_evidence_using_padding([asset + '-' + tags[0]], occurances[asset][0], interval_ms),  # noqa: E501
                    }],
                    "estimated_occurrence_loss": len(occurances[asset])*0.1,
                    "estimated_aep_loss": 17*len(occurances[asset]),
                    "estimated_life_reduction_days": 0,
                    "status": "Office",
                }
            )

    return ret
