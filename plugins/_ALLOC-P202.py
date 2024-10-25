# from datetime import datetime, timedelta
# from typing import Dict, List

# import pandas as pd

# from digital_punchlist.bazefield_api import (AllocationType, BazefieldAPI,
#                                              OutputItem)
from datetime import datetime
from typing import List

from digital_punchlist.bazefield_api import BazefieldAPI, OutputItem

TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%f0Z'


def run(
    api: BazefieldAPI,
    site: str,
    turbines: List[str],
    start_time: datetime,
    end_time: datetime,
) -> List[OutputItem]:
    ret: List[OutputItem] = []
    """
    occurances: Dict = {}
    allocation_type = "APCO-Parent"
    days_total = 7
    for turb in turbines:
        occurances[turb] = []

    min_duration: timedelta = timedelta(days=days_total)
    allocations: pd.DataFrame = api.get_allocations_from_asset_names(
        turbines, start_time[0], end_time[0],
        AllocationType.types[allocation_type],
        include_available=False
        )

    for alloc in allocations.values:
        alloc_wtg: str = alloc[list(allocations.columns).index('turbineName')]
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
        loss: float = alloc[list(allocations.columns).index('lostProduction')]
        duration = alloc_end - alloc_start
        category: str = alloc[list(allocations.columns).index('category.name')]
        cause: str = alloc[list(allocations.columns).index('rootCause')]
        if duration > min_duration:
            occurances[alloc_wtg].append({
                "allocStart": alloc_start,
                "allocEnd": alloc_end,
                "duration": duration,
                "loss": loss/1000,
                "category": category,
                "cause": cause
            })

    for asset in occurances:
        if len(occurances[asset]) > 0:
            tot_loss = 0
            for occ in occurances[asset]:
                if occ["loss"] > 0:
                    tot_loss += occ["loss"]
            ret.append(
                {
                    "asset": asset,
                    "priority": "ASAP",
                    "certainty": "Attend ASAP",
                    "occurrence_start": occurances[asset][0]["allocStart"],
                    "occurrence_end": occurances[asset][0]["allocEnd"],
                    "evidence": [{
                        "timestamp": 1,
                        "variable": str(occurances[asset][0]["category"]) + ": " + str(occurances[asset][0]["cause"]),  # noqa E501
                        "value": api.timedelta_to_num_days(
                            occurances[asset][0]["duration"]
                        ),
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
    """
    return ret
