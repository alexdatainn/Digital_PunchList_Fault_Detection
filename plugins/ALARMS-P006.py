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
    analysis_alarms: List[int] = [4034]
    # calc_duration_limit: float = 0.0001*(end_time[0]-start_time[0])
    filter_duration_min: float = timedelta(seconds=120)
    calc_count_limit: float = 14
    calc_durations: dict[str, dict] = {}
    for wtg_name in turbines:
        calc_durations[wtg_name] = {
            'totalDuration': timedelta(),
            'totalCount': 0,
            'alarmsFound': []
        }
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
            if (duration > filter_duration_min):
                calc_durations[event_wtg]['totalCount'] += 1
                calc_durations[event_wtg]['totalDuration'] += duration
                if alarm_code not in calc_durations[event_wtg]['alarmsFound']:
                    calc_durations[event_wtg]['alarmsFound'].append(alarm_code)

    for asset in calc_durations:
        # print(asset)
        if (
            # (calc_durations[asset]['totalDuration'] > calc_duration_limit)
            # or
            (calc_durations[asset]['totalCount'] >= calc_count_limit)
        ):
            loss_value = calc_durations[asset]['totalDuration'].total_seconds()/60/60*0.3*0.05  # noqa E501
            aep_loss_value = 1.2 # 5*0.3 - (24*2*0.03)/5   # the logic is== that converter takes 1 day to repauir and average life, MTBF, is 5 years, but when run ohot, the life is reduced by 30%  # noqa E501
            ret.append({
                "asset": asset,
                "priority": "Next Visit",
                "certainty": "Auto-Escalate",
                "occurrence_start": start_time[0],
                "occurrence_end": end_time[0],
                "evidence": [{
                    "timestamp": round(start_time[0].timestamp()*1000),
                    "variable": f"{calc_durations[asset]['alarmsFound'][0]}",  # noqa E501
                    "value": calc_durations[asset]['totalCount'],
                    "link": api.generate_alarm_log_link_from_evidence(
                        api.datetime_to_unix_time(start_time[0]),
                        api.datetime_to_unix_time(end_time[0]), asset
                    ),
                }],
                "estimated_occurrence_loss": loss_value,
                "estimated_aep_loss": aep_loss_value,
                "estimated_life_reduction_days": 0,
                "status": "ROC",
            })
        """else:
            ret.append({
                "asset": asset,
                "status": "Auto-close",
                "occurrence_end": end_time[0],
                "note": f"Number of alarm 4034 this week is {calc_durations[asset]['totalCount']} less than threshold[{calc_count_limit}]"  # noqa E501
            })
        """

    return ret
