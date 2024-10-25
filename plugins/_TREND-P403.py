from datetime import datetime  # , timedelta
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
    for turb in turbines:
        occurances[turb] = []
        values[turb] = []
    interval_ms: int = 6*600000
    tags: List[str] = ["PI-ProducedMWh", "PI-TheoreticalProdMWh", "IEC-OperationState"]  # noqa E501
    bin_lower_limit: float = 0.75
    bin_upper_limit: float = 0.90
    min_mwh: float = 0.5
    max_mwh: float = 2.5
    min_op_state: int = 4
    min_num_hours: int = 12

    measures_avg: pd.DataFrame = api.get_measurement_timeseries(
        turbines, tags, start_time[0], end_time[0], interval_ms=interval_ms,
        aggregate_type=AggregateType.time_average
        )

    for turb in turbines:
        if (
            (('v_' + turb + '-' + tags[0]) in measures_avg)
            and (('v_' + turb + '-' + tags[1]) in measures_avg)
            and (('v_' + turb + '-' + tags[2]) in measures_avg)
        ):
            for i, val in enumerate(measures_avg['v_' + turb + '-' + tags[2]]):
                timestamp = measures_avg['v_' + turb + '-' + tags[2]].index[i]
                op_state = measures_avg['v_' + turb + '-' + tags[2]][timestamp]
                act_mwh = measures_avg['v_' + turb + '-' + tags[0]][timestamp]
                theo_mwh = measures_avg['v_' + turb + '-' + tags[1]][timestamp]
                if (
                    (op_state == min_op_state)
                    and (act_mwh > min_mwh)
                    and (act_mwh < max_mwh)
                    and (theo_mwh > min_mwh)
                    and (theo_mwh < max_mwh)
                    and (act_mwh/theo_mwh > bin_lower_limit)
                    and (act_mwh/theo_mwh < bin_upper_limit)
                ):
                    occurances[turb].append(timestamp)
                    values[turb].append(act_mwh/theo_mwh)

    for asset in occurances:
        if len(values[asset]) > min_num_hours:
            ret.append(
                {
                    "asset": asset,
                    "priority": "Next PM",
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "evidence": [{
                        "timestamp": int(occurances[asset][0]),
                        "variable": f"{asset}-{tags[0]}",
                        "value": len(values[asset]),
                        "link": api.generate_trend_link_from_evidence_using_padding([asset + '-' + tags[0], asset + '-' + tags[1], asset + '-' + tags[2]], occurances[asset][0], interval_ms),  # noqa: E501
                    }],
                    "estimated_occurrence_loss": len(occurances[asset])*0.1,
                    "estimated_aep_loss": 17*len(occurances[asset]),
                    "estimated_life_reduction_days": 0,
                    "status": "ROC",
                }
            )

    return ret
