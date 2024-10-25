from datetime import datetime, timedelta
from typing import List

import pandas as pd

from digital_punchlist.bazefield_api import BazefieldAPI, OutputItem

# P160 Definition: Alarm 327 code.
# If sum of duration of alarm >= 10% of time monitoring > 100 times.
TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.%f0Z'


def run(
    api: BazefieldAPI,
    site: str,
    turbines: List[str],
    start_time: datetime,
    end_time: datetime,
) -> List[OutputItem]:
    ret: List[OutputItem] = []
    analysis_alarms: List[int] = [326]  # before was 327
    calc_duration_limit: float = 0.25*(end_time[0]-start_time[0])
    calc_count_limit: float = 100
    calc_durations: dict[str, dict] = {}

    filt_alarms: pd.DataFrame = api.get_filtered_alarms_from_asset_names(
        turbines, start_time[0], end_time[0], analysis_alarms
        )

    for alarm_event in filt_alarms.values:
        event_wtg: str = alarm_event[
            list(filt_alarms.columns).index('turbineName')
            ]
        alarm_code: int = alarm_event[
            list(filt_alarms.columns).index('code')
            ]
        if ((event_wtg in turbines) and (alarm_code in analysis_alarms)):
            if event_wtg not in calc_durations:
                calc_durations[event_wtg] = {
                    'totalDuration': timedelta(),
                    'totalCount': 0,
                    'alarmsFound': []
                    }
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
        print(asset)
        if (
            (sorted_durations[asset]['totalDuration'] > calc_duration_limit)
            or (sorted_durations[asset]['totalCount'] > calc_count_limit)
        ):
            print(sorted_durations[asset]['totalDuration'])
            ret.append(
                {
                    "asset": asset,
                    "priority": "Next PM",
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
                    "estimated_occurrence_loss": 28/52,  # noqa E501
                    "estimated_aep_loss": 28,  # noqa E501
                    "estimated_life_reduction_days": 0,
                    "status": "ROC",
                }
            )

    return ret
