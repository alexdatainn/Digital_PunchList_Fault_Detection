from datetime import datetime  # , timedelta
from typing import Dict, List

import pandas as pd
from digital_punchlist.bazefield_api import (AggregateType, BazefieldAPI,
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
    values: Dict = {}
    for turb in turbines:
        occurances[turb] = []
        values[turb] = {"totalDurationHours": 0, "totalLoss": 0, "timestamps": []}  # noqa E501
    interval_ms = 6*600000
    min_temp = 4
    min_num_hours = 0.1*7*24
    analysis_alarms: List[int] = [600]
    tags: List[str] = ["AmbientTemperature"]

    loss_series = api.get_hourly_loss_timeseries(
        turbines, start_time[0], end_time[0])

    measures: pd.DataFrame = api.get_measurement_timeseries(
        turbines, tags, start_time[0], end_time[0], interval_ms,
        aggregate_type=AggregateType.time_average)
    median_series = pd.DataFrame(measures.median(axis='columns'))
    median_series.columns = ['median_' + tags[0]]
    measures = measures.join(median_series)

    filt_alarms: pd.DataFrame = api.get_filtered_alarms_from_asset_names(
        turbines, start_time[0], end_time[0], analysis_alarms)

    for i in range(len(filt_alarms)):
        flag = 0
        turb_name = filt_alarms['turbineName'][i]
        alarm_start = api.datetime_to_unix_time(datetime.strptime(filt_alarms['start'][i], TIMESTAMP_FORMAT))  # noqa E501
        alarm_end = api.datetime_to_unix_time(end_time[0])
        if isinstance(filt_alarms['end'][i], str):
            alarm_end = api.datetime_to_unix_time(datetime.strptime(filt_alarms['end'][i], TIMESTAMP_FORMAT))  # noqa E501
            for j in range(len(median_series)):
                timestamp = median_series['median_' + tags[0]].index[j]
                median_val = median_series['median_' + tags[0]][timestamp]
                loss_val = loss_series['v_' + turb + "-PI-TheoreticalProdMWh"]
                if (
                    (timestamp < alarm_end)
                    and (timestamp > alarm_start)
                    and (median_val > min_temp)
                    and (timestamp in loss_val)
                ):
                    occurances[turb_name].append({
                        "timestamp": timestamp,
                        "value": median_val,
                        "ref_value": min_temp
                    })
                    flag += 1
                    if loss_val[timestamp] > 0:
                        values[turb_name]["totalLoss"] += loss_val[timestamp]
        if flag > 0:
            values[turb_name]["totalDurationHours"] += (alarm_end - alarm_start)/interval_ms  # noqa E501
            values[turb_name]["timestamps"].append({"start": alarm_start, "end": alarm_end})  # noqa E501

    for asset in values:
        num_hours = values[asset]["totalDurationHours"]
        loss_mwh = values[asset]["totalLoss"]
        if num_hours > min_num_hours:
            if not (loss_mwh > 0):
                loss_mwh = 0
            ret.append(
                {
                    "asset": asset,
                    "priority": "Next Visit",
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "evidence": [{
                        "timestamp": values[asset]["timestamps"][0]["start"],
                        "variable": f"{asset}-{tags[0]}",
                        "value": round(num_hours, 1),
                        "ref_value": min_num_hours,
                        "link": api.generate_power_curve_link_from_evidence(api.datetime_to_unix_time(start_time[0]), api.datetime_to_unix_time(end_time[0]), asset)  # noqa E501
                    }],
                    "estimated_occurrence_loss": loss_mwh,
                    "estimated_aep_loss": round(52*loss_mwh/2),
                    "estimated_life_reduction_days": 0,
                    "status": "Office",
                }
            )

    return ret
