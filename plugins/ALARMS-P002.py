from datetime import datetime, timedelta
from typing import List

import pandas as pd

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
    # only impacts if wind is high enough
    analysis_alarms: List[int] = [5013, 5014, 5015, 5016]
    calc_duration_limit: float = 0.01*(end_time[0]-start_time[0])  # this becomes 30 minutes  # noqa E501
    calc_durations: dict[str, dict] = {}

    filt_alarms: pd.DataFrame = api.get_filtered_alarms_from_asset_names(
        turbines, start_time[0], end_time[0], analysis_alarms
        )

    for wtg_key in turbines:
        if wtg_key not in calc_durations:
            calc_durations[wtg_key] = {
                'totalDuration': timedelta(),
                'totalCount': 0,
                'alarmsFound': []
            }

    for alarm_event in filt_alarms.values:
        event_wtg: str = alarm_event[
            list(filt_alarms.columns).index('turbineName')
            ]
        alarm_code: int = alarm_event[
            list(filt_alarms.columns).index('code')
            ]
        if ((event_wtg in turbines) and (alarm_code in analysis_alarms)):
            if (
                'end' in list(filt_alarms.columns)
                and isinstance(
                    alarm_event[list(filt_alarms.columns).index('end')],
                    str)
            ):
                duration = (
                    datetime.strptime(
                        alarm_event[list(filt_alarms.columns).index('end')],
                        TIMESTAMP_FORMAT) -
                    datetime.strptime(
                        alarm_event[list(filt_alarms.columns).index('start')],
                        TIMESTAMP_FORMAT
                        )
                )
            else:
                duration = (
                    end_time[0] -
                    datetime.strptime(
                        alarm_event[list(filt_alarms.columns).index('start')],
                        TIMESTAMP_FORMAT)
                    )
            calc_durations[event_wtg]['totalCount'] += 1
            calc_durations[event_wtg]['totalDuration'] += duration
            if alarm_code not in calc_durations[event_wtg]['alarmsFound']:
                calc_durations[event_wtg]['alarmsFound'].append(alarm_code)
    # sorting not necessary but useful for developer analysis
    sorted_durations = dict(sorted(
        calc_durations.items(),
        key=lambda item: item[1]['totalDuration'],
        reverse=True))

    for asset in sorted_durations:
        if (
            (sorted_durations[asset]['totalDuration'] > calc_duration_limit)
            and not (sorted_durations[asset]['totalDuration'] == timedelta(0))
        ):
            ret.append(
                {
                    "asset": asset,
                    "priority": "Next Visit",
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "evidence": [{
                        "timestamp": round(start_time[0].timestamp()*1000),
                        "variable": f"{sorted_durations[asset]['alarmsFound'][0]}",  # noqa E501
                        "value": sorted_durations[asset]['totalCount'],
                        "link": api.generate_alarm_log_link_from_evidence(
                            api.datetime_to_unix_time(start_time[0]),
                            api.datetime_to_unix_time(end_time[0]), asset
                        ),
                    }],
                    "estimated_occurrence_loss": 500/52,  # noqa E501
                    "estimated_aep_loss": 500,  # noqa E501
                    "estimated_life_reduction_days": 0,
                    "status": "ROC",
                }
            )
        else:
            ret.append(
                {
                    "asset": asset,
                    "status": "Auto-close",
                    "occurrence_end": end_time[0],
                    "note": "Duration of alarms 5013, 5014, 5015, 5016 below threshold during this week"  # noqa E501
                }
            )

    return ret
