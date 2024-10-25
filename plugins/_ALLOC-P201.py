from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

from digital_punchlist.bazefield_api import (AllocationType, BazefieldAPI,
                                             OutputItem)

TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%f0Z'


def run(
    api: BazefieldAPI,
    site: str,
    turbines: List[str],
    start_time: datetime,
    end_time: datetime,
) -> List[OutputItem]:
    ret: List[OutputItem] = []
    occurances: Dict = {}
    allocation_type = "APCO-Parent"
    days_span = 1
    days_total = 7
    for turb in turbines:
        occurances[turb] = []
        for d in range(days_total):
            occurances[turb].append([])
    min_duration: timedelta = timedelta(minutes=4, seconds=59)
    allocations: pd.DataFrame = api.get_allocations_from_asset_names(
        turbines, start_time[0], end_time[0],
        AllocationType.types[allocation_type],
        include_available=False
        )

    for i in range(days_total):
        day_start = start_time[0] + timedelta(days=i)
        day_end = start_time[0] + timedelta(days=i+days_span)
        for alloc in allocations.values:
            alloc_wtg: str = alloc[list(allocations.columns).index('turbineName')]  # noqa E501
            alloc_start: datetime = datetime.strptime(
                alloc[list(allocations.columns).index('start')],
                TIMESTAMP_FORMAT
            )
            try:
                alloc_end: datetime = datetime.strptime(
                    alloc[list(allocations.columns).index('end')],
                    TIMESTAMP_FORMAT
                )
            except ValueError:
                alloc_end: datetime = datetime.now()
            loss: float = alloc[list(allocations.columns).index('lostProduction')]  # noqa E501
            duration = alloc_end - alloc_start
            if (
                (duration > min_duration)
                and (alloc_start > day_start)
                and (alloc_start < day_end)
            ):
                occurances[alloc_wtg][i].append({
                    "allocStart": alloc_start,
                    "allocEnd": alloc_end,
                    "dayStart": day_start,
                    "dayEnd": day_end,
                    "duration": duration,
                    "loss": loss/1000
                })

    for asset in occurances:
        flag = 0
        day_flags = []
        tot_loss = 0
        for j in range(days_total):
            if len(occurances[asset][j]) > 23:
                flag += len(occurances[asset][j])
                day_flags.append({j: len(occurances[asset][j])})
                for occ in occurances[asset][j]:
                    if occ["loss"] > 0:
                        tot_loss += occ["loss"]
        if flag > 0:
            ret.append(
                {
                    "asset": asset,
                    "priority": "Next Visit",
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0] + timedelta(
                        days=list(day_flags[0].keys())[0]
                        ),
                    "occurrence_end": end_time[0] + timedelta(
                        days=list(day_flags[0].keys())[0]
                        ),
                    "evidence": [{
                        "timestamp": 1,
                        "variable": "",
                        "value": list(day_flags[0].values())[0],
                        "link": api.generate_allocation_link_from_evidence(
                            api.datetime_to_unix_time(start_time[0]),
                            api.datetime_to_unix_time(end_time[0]),
                            allocation_type, asset, include_available=False
                        ),
                    }],
                    "estimated_occurrence_loss": round(tot_loss, 1),
                    "estimated_aep_loss": 0,
                    "estimated_life_reduction_days": 0,
                    "status": "ROC",
                }
            )

    return ret
