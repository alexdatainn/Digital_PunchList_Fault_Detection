from datetime import datetime, timedelta
from typing import Dict, List

import numpy as np
import pandas as pd

from digital_punchlist.bazefield_api import (AggregateType, BazefieldAPI,
                                             OutputItem)

TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.0000000Z'


def run(
    api: BazefieldAPI,
    site: str,
    turbines: List[str],
    start_time: datetime,
    end_time: datetime,
) -> List[OutputItem]:
    ret_ens: List[OutputItem] = []
    ret: List[OutputItem] = []
    enable: int = 1
    occurances: Dict = {}
    stable_data: Dict = {}
    ens_found: List = []
    for turb in turbines:
        occurances[turb] = []
        stable_data[turb] = []
    interval_ms: int = 600000
    min_temp: int = -150
    max_temp: int = 150
    max_diff: int = 8
    min_state: int = 4
    min_power: float = 100
    state_tags: List[str] = ["IEC-OperationState"]
    power_tags: List[str] = ["ActivePower"]
    min_duration: float = 48  # hours
    if site in ["SENT", "SNDY", "MN", "BLH", "MAV"]:
        tags: List[str] = ["Temp.MB-Rear"]
    elif site in ["RLWEP", "SL"]:
        tags: List[str] = ["TempMainBearing1"]
    elif site in ["DFS", "OWF"]:
        tags: List[str] = ["MainBearingTemp"]
    else:
        tags: List[str] = ["MainBearingTemp"]
        enable = 0
    enable = 1
    site_full_name = api.get_site_full_name_from_short_name(site)

    ensight_events: dict = api.get_ensight_events_for_wtg_list(
        turbines, site_full_name,
        (start_time[0]-timedelta(days=1)).strftime(TIMESTAMP_FORMAT),
        (end_time[0]+timedelta(days=1)).strftime(TIMESTAMP_FORMAT)
    )

    for asset in ensight_events:
        if asset['hcCode'] == "HC_02_03_32_04":
            ens_found.append(asset["wtgName"])
            if ((asset["severityLevel"] == 0) or (asset["severityLevel"] == 1)):
                priority = "Monitor"
            elif (asset["severityLevel"] == 2):
                priority = "Next PM"
            elif (asset["severityLevel"] == 3):
                priority = "Next Visit"
            elif (asset["severityLevel"] > 3):
                priority = "ASAP"
            else:
                priority = "Next PM"
            ret_ens.append(
                {
                    "asset": asset['wtgName'],
                    "priority": priority,
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "estimated_occurrence_loss": -1*asset['aepLoss']/52,
                    "estimated_aep_loss": -1*asset['aepLoss'],
                    "estimated_life_reduction_days": 0,
                    "status": "ROC",
                    "evidence": [{
                        "timestamp": api.datetime_to_unix_time(start_time[0]),
                        "variable": "Ensight: Sensors",
                        "value": -1*asset['aepLoss'],
                        "link":  api.get_ensight_link_from_site_and_type(site, "sensors"),  # noqa: E501
                    }],
                }
            )

    if enable > 0:
        measures: pd.DataFrame = api.get_measurement_timeseries(
            turbines, tags, start_time[0], end_time[0], interval_ms,
            aggregate_type=AggregateType.time_average
        )
        states: pd.DataFrame = api.get_measurement_timeseries(
            turbines, state_tags+power_tags, start_time[0], end_time[0], interval_ms,
            aggregate_type=AggregateType.time_average
        )
        wtg_model_lookup = api.get_asset_model_lookup_at_site(site)
        exclude_wtgs: List[str] = []
        temp_tags: List[str] = []
        asset_power_tags: List[str] = []
        asset_state_tags: List[str] = []
        for turb in turbines:
            if ('v_' + turb + '-' + tags[0]) in measures:
                temp_tags.append('v_' + turb + '-' + tags[0])
            if ('v_' + turb + '-' + power_tags[0]) in states:
                asset_power_tags.append('v_' + turb + '-' + power_tags[0])
            if ('v_' + turb + '-' + state_tags[0]) in states:
                asset_state_tags.append('v_' + turb + '-' + state_tags[0])
        num_count = 0
        if (len(measures.columns) > 0):
            for k, val2 in enumerate(measures[temp_tags[0]]):
                timestamp = measures[temp_tags[0]].index[k]

                all_running_assets_at_time = []

                for turb2 in turbines:
                    if ('v_' + turb2 + '-' + tags[0] in measures):
                        if (
                            ('v_' + turb2 + '-' + state_tags[0] in states) and
                            (states['v_' + turb2 + '-' + state_tags[0]][timestamp] == min_state) and
                            ('v_' + turb2 + '-' + power_tags[0] in states) and
                            (states['v_' + turb2 + '-' + power_tags[0]][timestamp] >= min_power)
                        ):
                            all_running_assets_at_time.append('v_' + turb2 + '-' + tags[0])
                        else:
                            measures['v_' + turb2 + '-' + tags[0]][timestamp] = np.nan
                            num_count+=1
                            # print("Changing val")
        print(f"changed total {num_count} values")

        # median_series = pd.DataFrame((measures[temp_tags]).median(axis='columns'))  # noqa E501
        median_series = pd.DataFrame(measures[temp_tags].median(axis='columns'))  # noqa E501
        median_series.columns = ['median_' + tags[0]]
        measures = measures.join(median_series)

        for turb in turbines:
            if (
                (('v_' + turb + '-' + tags[0]) in measures) and
                (('v_' + turb + '-' + power_tags[0]) in states) and
                (('v_' + turb + '-' + state_tags[0]) in states) and
                (('median_' + tags[0]) in measures)
            ):
                for i, val in enumerate(measures['v_' + turb + '-' + tags[0]]):
                    timestamp = measures['median_' + tags[0]].index[i]
                    median_val = measures['median_' + tags[0]][timestamp]
                    power_val = states['v_' + turb + '-' + power_tags[0]][timestamp]  # noqa E501
                    state_val = states['v_' + turb + '-' + state_tags[0]][timestamp]  # noqa E501
                    if (
                        ((val-median_val) > max_diff)
                        and (median_val < max_temp) and (median_val > min_temp)
                        and (val < max_temp) and (val > min_temp)
                        and (state_val == min_state)
                        and (power_val >= min_power)
                    ):
                        occurances[turb].append({
                            "timestamp": timestamp,
                            "value": val,
                            "referenceValue": median_val
                        })
                    elif (
                        ((val-median_val) < max_diff)
                        and (abs(val-median_val) < max_diff) # for stable data
                        and (median_val < max_temp) and (median_val > min_temp)
                        and (val < max_temp) and (val > min_temp)
                        and (state_val == min_state)
                        and (power_val >= min_power)
                    ):
                        stable_data[turb].append({
                            "timestamp": timestamp,
                            "value": val,
                            "referenceValue": median_val
                        })
                    elif (
                        (median_val > max_temp) or (median_val < min_temp)
                        or (val > max_temp) or (val < min_temp)
                    ):
                        print("FOUND BAD TEMP VALUE/OUTLIER")
            else:
                print("ERROR CHECK TAG DATA EXIST FOR "+turb+"-"+tags[0]+" @ "+site)  # noqa E501
                exclude_wtgs.append(turb)
                print('')
        stable_wtgs = []
        best_wtg = 0
        for stable_asset in stable_data:
            if (
                (len(occurances[stable_asset]) == 0)
                and (stable_asset not in exclude_wtgs)
            ):  # noqa E501
                if ((len(stable_wtgs) > 0) and (len(stable_data[stable_asset]) > best_wtg)):  # noqa E501
                    stable_wtgs.insert(0, stable_asset + '-' + tags[0])
                    best_wtg = len(stable_data[stable_asset])
                elif len(stable_wtgs) == 0:
                    best_wtg = len(stable_data[stable_asset])
                    stable_wtgs.append(stable_asset + '-' + tags[0])

        for wtg in occurances:
            dur = len(occurances[wtg])/6
            dur_round = round(dur)
            if dur >= min_duration:
                avg_dif = 0
                for occ_dict in occurances[wtg]:
                    avg_dif += (occ_dict["referenceValue"]-occ_dict["value"])
                if len(stable_wtgs) > 1:
                    link_wtgs = [wtg + '-' + tags[0], stable_wtgs[0], wtg + '-' + power_tags[0]]  # noqa E501
                elif len(stable_wtgs) > 0:
                    link_wtgs = [wtg + '-' + tags[0], stable_wtgs[0], wtg + '-' + power_tags[0]]  # noqa E501
                else:
                    link_wtgs = [wtg + '-' + tags[0], wtg + '-' + power_tags[0]]  # noqa E501
                if wtg not in ens_found:
                    ret.append(
                        {
                            "asset": wtg,
                            "priority": "Next PM",
                            "certainty": "Monitor",
                            "occurrence_start": start_time[0],
                            "occurrence_end": end_time[0],
                            "evidence": [{
                                "timestamp": int(occurances[wtg][0]["timestamp"]),  # noqa E501
                                "variable": f"# Hrs {dur_round}, Avg Temp Above Median: ",  # noqa E501
                                "value": round(-10*avg_dif/len(occurances[wtg]))/10,  # noqa E501 
                                "link": api.generate_trend_link_from_evidence_using_start_and_end(  # noqa E501
                                    link_wtgs,
                                    api.datetime_to_unix_time(start_time[0]),
                                    api.datetime_to_unix_time(end_time[0])
                                ),
                            }],
                            "estimated_occurrence_loss": 0,
                            "estimated_aep_loss": 0,
                            "estimated_life_reduction_days": 0,
                            "status": "ROC",
                        }
                    )

    return ret_ens + ret
