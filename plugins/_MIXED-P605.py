import json
from datetime import datetime
from typing import Dict, List

import pandas as pd

from digital_punchlist.bazefield_api import (AggregateType, BazefieldAPI,
                                             OutputItem)


# make wide into fixed with 
def run(
    api: BazefieldAPI, site: str, turbines: List[str],
    start_time: datetime, end_time: datetime,
) -> List[OutputItem]:
    



    def prepare_raw_data(wtg_measures, assets, wtg_model_lookup):
        for asset in assets:
            print(asset)
            wtg_measures["v_" + asset +"-lookup_power"] = 0
            wtg_measures["v_" + asset + "-RUNNING"] = 0
            if (
                (('v_' + asset + '-WindSpeed') in wtg_measures) and
                (('v_' + asset + '-ActivePower') in wtg_measures) and
                (('v_' + asset + '-IEC-OperationState') in wtg_measures)
                ):
                wtg_measures["v_" + asset +"-ws_bin"] = round(wtg_measures["v_" + asset +"-WindSpeed"]*2)/2
                for i, val in enumerate(wtg_measures['v_' + asset + '-ws_bin']):
                    timestamp = wtg_measures['v_' + asset + '-ws_bin'].index[i]
                    ws_bin = float(wtg_measures['v_' + asset + '-ws_bin'][timestamp])
                    if (ws_bin > 0 and ws_bin in wtg_model_lookup[asset]['lookupWindBin']):
                        ws_ind = wtg_model_lookup[asset]['lookupWindBin'].index(ws_bin)
                    else:
                        ws_ind = 0
                    fact_pow = wtg_model_lookup[asset]['lookupPower'][ws_ind]
                    # wtg_measures.at["v_" + asset + "-lookup_power", timestamp] = fact_pow
                    # wtg_measures.loc[:, ("v_" + asset +"-lookup_power", timestamp)] = fact_pow
                    wtg_measures["v_" + asset +"-lookup_power"][timestamp] = fact_pow
                    wtg_pow = wtg_measures["v_" + asset +"-ActivePower"][timestamp]
                    iec_state = wtg_measures["v_" + asset +"-IEC-OperationState"][timestamp]
                    if ( (iec_state == 4) and (wtg_pow > 10) ):
                        wtg_measures["v_" + asset +"-RUNNING"][timestamp] = 1
                        # wtg_measures.loc[:, ("v_" + asset +"-RUNNING", timestamp)] = 1
                        # wtg_measures.at["v_" + asset + "-RUNNING", timestamp] = 1
        return wtg_measures

    def bucket_causes_sgre(cause_df, measure_df, assets, tag_lookup, site):
        for wtg in assets:
            cause_df["v_" + wtg + "-BUCKETNUMBER"] = -1
            component_temp_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_temp_limit_control"]
            cosphi_lim_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_remote_power_factor_control"]
            act_pow_control_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_active_power_control"]
            pow_scada_control_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_scada_power_limit"]
            freq_control_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_actpow_limit_by_freq"]
            noise_derate_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_noise_control_derate"]
            ambient_temp_measure_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_amb_temp"]
            ws_measure_key = 'v_'+wtg+'-WindSpeed'
            asset_running_key = 'v_'+wtg+'-RUNNING'
            lookup_power_key = 'v_'+wtg+'-lookup_power'
            if ((ws_measure_key in measure_df) and (ambient_temp_measure_key in measure_df)):
                for i, val in enumerate(cause_df['v_' + wtg + '-BUCKETNUMBER']):
                    timestamp = cause_df['v_' + wtg + '-BUCKETNUMBER'].index[i]
                    running = measure_df[asset_running_key][timestamp]
                    ws_val = measure_df[ws_measure_key][timestamp]
                    amb_temp = measure_df[ambient_temp_measure_key][timestamp]
                    lookup_pow = measure_df[lookup_power_key][timestamp]
                    if ((component_temp_flag_key in cause_df) and (cause_df[component_temp_flag_key][timestamp] > 0)):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 1
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 1
                    elif (
                        (cosphi_lim_flag_key in cause_df and  cause_df[cosphi_lim_flag_key][timestamp]==True)
                        or (act_pow_control_flag_key in cause_df and  cause_df[act_pow_control_flag_key][timestamp]==True)
                        or (pow_scada_control_flag_key in cause_df and  cause_df[pow_scada_control_flag_key][timestamp]==True)
                        or (freq_control_flag_key in cause_df and cause_df[freq_control_flag_key][timestamp] == 1)
                        or (noise_derate_flag_key in cause_df and  cause_df[noise_derate_flag_key][timestamp]==1)
                        # or (active_crowbar_flag_key in cause_df and not cause_df[active_crowbar_flag_key][timestamp])

                    ):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 2
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 2
                    elif (running and amb_temp <= 4):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 3
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 3
                    elif ((not ws_val < 30) or (not ws_val > 0) or (not amb_temp < 30) or (not amb_temp > 4)): 
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 4
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 4
                    elif running==1 and ws_val > 0:
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 0
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 0
        return cause_df

    def bucket_causes_gam(cause_df, measure_df, assets, tag_lookup, site_sp_df, site):
        for wtg in assets:
            cause_df["v_" + wtg + "-BUCKETNUMBER"] = -1
            component_temp_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_temp_limit_control"]
            cosphi_lim_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_remote_power_factor_control"]
            act_pow_control_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_active_power_control"]
            pow_scada_control_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_scada_power_limit"]
            freq_control_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_actpow_limit_by_freq"]
            noise_derate_flag_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_noise_control_derate"]
            ambient_temp_measure_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_amb_temp"]
            site_sp_key = 'v_'+site+'-'+tag_lookup[site]["site_setpoint"]
            site_sp_threshold = tag_lookup[site]["site_sp_threshold"]
            ws_measure_key = 'v_'+wtg+'-WindSpeed'
            asset_running_key = 'v_'+wtg+'-RUNNING'
            lookup_power_key = 'v_'+wtg+'-lookup_power'
            if ((ws_measure_key in measure_df) and (ambient_temp_measure_key in measure_df)):
                for i, val in enumerate(cause_df['v_' + wtg + '-BUCKETNUMBER']):
                    timestamp = cause_df['v_' + wtg + '-BUCKETNUMBER'].index[i]
                    running = measure_df[asset_running_key][timestamp]
                    ws_val = measure_df[ws_measure_key][timestamp]
                    amb_temp = measure_df[ambient_temp_measure_key][timestamp]
                    lookup_pow = measure_df[lookup_power_key][timestamp]
                    if ((component_temp_flag_key in cause_df) and (cause_df[component_temp_flag_key][timestamp] > 0)):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 1
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 1
                    elif (
                        (cosphi_lim_flag_key in cause_df and  cause_df[cosphi_lim_flag_key][timestamp]==True)
                        or (act_pow_control_flag_key in cause_df and  cause_df[act_pow_control_flag_key][timestamp]==True)
                        or (pow_scada_control_flag_key in cause_df and  cause_df[pow_scada_control_flag_key][timestamp]==True)
                        or (freq_control_flag_key in cause_df and cause_df[freq_control_flag_key][timestamp] == 1)
                        or (noise_derate_flag_key in cause_df and  cause_df[noise_derate_flag_key][timestamp]==1)
                        # or (active_crowbar_flag_key in cause_df and not cause_df[active_crowbar_flag_key][timestamp])

                    ):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 2
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 2
                    elif ((site_sp_key in site_sp_df) and not (site_sp_df[site_sp_key][timestamp] == site_sp_threshold)):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 5
                    elif (running and amb_temp <= 4):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 3
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 3
                    elif ((not ws_val < 30) or (not ws_val > 0) or (not amb_temp < 30) or (not amb_temp > 4)): 
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 4
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 4
                    elif running == 1 and ws_val > 0:
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 0
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 0
        return cause_df

    def bucket_causes_site_setpoint(measure_df, assets, tag_lookup, site_sp_df, site):
        for wtg in assets:
            measure_df["v_" + wtg + "-BUCKETNUMBER"] = -1
            ambient_temp_measure_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_amb_temp"]
            site_sp_key = 'v_'+site+'-'+tag_lookup[site]["site_setpoint"]
            site_sp_threshold = tag_lookup[site]["site_sp_threshold"]
            ws_measure_key = 'v_'+wtg+'-WindSpeed'
            asset_running_key = 'v_'+wtg+'-RUNNING'
            lookup_power_key = 'v_'+wtg+'-lookup_power'
            if ((ws_measure_key in measure_df) and (ambient_temp_measure_key in measure_df)):
                for i, val in enumerate(measure_df['v_' + wtg + '-BUCKETNUMBER']):
                    timestamp = measure_df['v_' + wtg + '-BUCKETNUMBER'].index[i]
                    running = measure_df[asset_running_key][timestamp]
                    ws_val = measure_df[ws_measure_key][timestamp]
                    amb_temp = measure_df[ambient_temp_measure_key][timestamp]
                    lookup_pow = measure_df[lookup_power_key][timestamp]
                    
                    if ((site_sp_key in site_sp_df) and (site_sp_df[site_sp_key][timestamp] < site_sp_threshold)):
                        measure_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 5
                    elif (running and amb_temp <= 4):
                        measure_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 3
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 3
                    elif ((not ws_val < 30) or (not ws_val > 0) or (not amb_temp < 30) or (not amb_temp > 4)): 
                        measure_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 4
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 4
                    elif running==1 and ws_val > 0 and (site_sp_key in site_sp_df) and (site_sp_df[site_sp_key][timestamp] >= 0):
                            print(site_sp_df[site_sp_key], ws_val, amb_temp, running)
                            measure_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 0
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 0
        return measure_df

    def bucket_causes_vestas(cause_df, measure_df, assets, tag_lookup, site):
        for wtg in assets:
            cause_df["v_" + wtg + "-BUCKETNUMBER"] = -1
            derate_state_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_vestas_derate_state"]
            ambient_temp_measure_key = 'v_'+wtg+'-'+tag_lookup[site]["wtg_amb_temp"]
            ws_measure_key = 'v_'+wtg+'-WindSpeed'
            asset_running_key = 'v_'+wtg+'-RUNNING'
            lookup_power_key = 'v_'+wtg+'-lookup_power'
            for i, val in enumerate(cause_df['v_' + wtg + '-BUCKETNUMBER']):
                if ((ws_measure_key in measure_df) and (ambient_temp_measure_key in measure_df)):
                    timestamp = cause_df['v_' + wtg + '-BUCKETNUMBER'].index[i]
                    running = measure_df[asset_running_key][timestamp]
                    ws_val = measure_df[ws_measure_key][timestamp]
                    amb_temp = measure_df[ambient_temp_measure_key][timestamp]
                    lookup_pow = measure_df[lookup_power_key][timestamp]
                    if ((derate_state_key in cause_df) and (cause_df[derate_state_key][timestamp] >= 0)):
                        derate_state = cause_df[derate_state_key][timestamp]
                        if ((derate_state == 0) or (derate_state == 20) or (derate_state == 56)):
                            cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 0
                        elif ((derate_state == 1) or (derate_state == 2) or (derate_state == 3) or (derate_state == 4) or (derate_state == 21) or (derate_state == 22) or (derate_state == 23) or (derate_state == 24) or (derate_state == 25) or (derate_state == 26) or (derate_state == 28) or (derate_state == 29) or (derate_state == 30) or (derate_state == 31) or (derate_state == 32) or (derate_state == 35) or (derate_state == 36) or (derate_state == 37) or (derate_state == 39) or (derate_state == 40) or (derate_state == 41) or (derate_state == 42) or (derate_state == 44) or (derate_state == 45) or (derate_state == 49) or (derate_state == 51) or (derate_state == 52) or (derate_state == 53) or (derate_state == 54)):
                            cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 1
                        elif ((derate_state == 5) or (derate_state == 6) or (derate_state == 7) or (derate_state == 8) or (derate_state == 9) or (derate_state == 10) or (derate_state == 11) or (derate_state == 12) or (derate_state == 12) or (derate_state == 14) or (derate_state == 15) or (derate_state == 16) or (derate_state == 17) or (derate_state == 18) or (derate_state == 33) or (derate_state == 34) or (derate_state == 38) or (derate_state == 43) or (derate_state == 55)):
                            cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 2
                        elif ((derate_state == 19) or (derate_state == 27) or (derate_state == 50)):
                            cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 4
                    elif ((not ws_val < 30) or (not ws_val > 0) or (not amb_temp < 30) or (not amb_temp > 4)): 
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 4
                    elif (running and amb_temp <= 4):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 3
                        # cause_df.loc[:, ("v_" + wtg + "-BUCKETNUMBER", timestamp)] = 3
                    elif (running == 1) and (ws_val > 0) and (derate_state_key in cause_df) and (cause_df[derate_state_key][timestamp] == 0):
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 0
        return cause_df

    def check_for_underperformance(measure_df, cause_df, assets, power_limit, ws_min):
        for wtg in assets:
            cause_df["v_" + wtg + "-CONDITION_OK"] = 0
            cause_df["v_" + wtg + "-POWER_GAP_FLAG"] = 0
            cause_df["v_" + wtg + "-POWER_GAP_VAL"] = 0
            if (('v_'+wtg+'-WindSpeed' in measure_df) and ('v_'+wtg+'-ActivePower' in measure_df)):
                for i, val in enumerate(cause_df['v_' + wtg + '-BUCKETNUMBER']):
                    timestamp = cause_df['v_' + wtg + '-BUCKETNUMBER'].index[i]
                    bucket = cause_df['v_'+wtg+'-BUCKETNUMBER'][timestamp]
                    running = measure_df['v_'+wtg+'-RUNNING'][timestamp]
                    ws_val = measure_df['v_'+wtg+'-WindSpeed'][timestamp]
                    act_pow = measure_df['v_'+wtg+'-ActivePower'][timestamp]
                    fact_pow = measure_df['v_'+wtg+'-lookup_power'][timestamp]
                    if ((running == 1) and (ws_val >= ws_min) and ((bucket == 0) or (bucket == 1))):
                        cause_df["v_" + wtg + "-CONDITION_OK"][timestamp] = 1
                        # cause_df.loc[:, ("v_" + wtg + "-CONDITION_OK", timestamp)] = 1
                        if ((fact_pow > 0) and ((fact_pow-act_pow)/fact_pow > power_limit)):
                            # cause_df.loc[:, ("v_" + wtg + "-POWER_GAP_FLAG", timestamp)] = 1
                            # cause_df.loc[:, ("v_" + wtg + "-POWER_GAP_VAL", timestamp)] = fact_pow-act_pow
                            cause_df["v_" + wtg + "-POWER_GAP_FLAG"][timestamp] = 1
                            cause_df["v_" + wtg + "-POWER_GAP_VAL"][timestamp] = fact_pow-act_pow

        return cause_df
        
    interval_ms: int = 600000
    power_gap_limit = 0.05  # flag if difference 4% of total expected
    min_condition_passes = 20
    min_condition_ratio = 0.15
    min_ws_limit = 12
    ret: List[OutputItem] = []
    wtg_derate_tags: List[str] = []
    with open('./configs/derate_tag_lookup.json', 'r') as f:
        tag_lookup = json.load(f)
        for der_key in tag_lookup[site]["derateKeys"]:
            wtg_derate_tags.append(tag_lookup[site][der_key])
    if ((site == "MAV") or (site == "BLH")):
        if (site == "MAV"):
            turbines = ["MAV-WTGT101", "MAV-WTGT102", "MAV-WTGT103", "MAV-WTGT104", "MAV-WTGT105", "MAV-WTGT116", "MAV-WTGT141", "MAV-WTGT142", "MAV-WTGT143", "MAV-WTGT144", "MAV-WTGT145", "MAV-WTGT146", "MAV-WTGT147", "MAV-WTGT148", "MAV-WTGT149", "MAV-WTGT150"]
        wtg_model_lookup = api.get_asset_model_lookup_at_site(site)
        wtg_tags: List[str] = [tag_lookup[site]["wtg_state"], tag_lookup[site]["wtg_power"], tag_lookup[site]["wtg_windspeed"], tag_lookup[site]["wtg_amb_temp"]]

        wtg_derate_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_derate_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.bool)

        wtg_measure_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        
        df1 = prepare_raw_data(wtg_measure_df, turbines, wtg_model_lookup)
        df2 = bucket_causes_sgre(wtg_derate_df, wtg_measure_df, turbines, tag_lookup, site)

        df1_ret = check_for_underperformance(df1, df2, turbines, power_gap_limit, min_ws_limit)
    elif (site in ["OWF", "SUGR", "DFS"]):
        wtg_model_lookup = api.get_asset_model_lookup_at_site(site)
        wtg_tags: List[str] = [tag_lookup[site]["wtg_state"], tag_lookup[site]["wtg_power"], tag_lookup[site]["wtg_windspeed"], tag_lookup[site]["wtg_amb_temp"]]

        wtg_derate_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_derate_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)

        wtg_measure_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        
        df1 = prepare_raw_data(wtg_measure_df, turbines, wtg_model_lookup)
        df2 = bucket_causes_vestas(wtg_derate_df, wtg_measure_df, turbines, tag_lookup, site)

        df1_ret = check_for_underperformance(df1, df2, turbines, power_gap_limit, min_ws_limit)
    
    elif (site in ["SENT", "SNDY", "MN"]):
        wtg_model_lookup = api.get_asset_model_lookup_at_site(site)
        wtg_tags: List[str] = [tag_lookup[site]["wtg_state"], tag_lookup[site]["wtg_power"], tag_lookup[site]["wtg_windspeed"], tag_lookup[site]["wtg_amb_temp"]]

        wtg_derate_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_derate_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.bool)

        wtg_measure_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        
        site_setpoint_df =  api.get_measurement_timeseries([site], [tag_lookup[site]["site_setpoint"]], start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)

        df1 = prepare_raw_data(wtg_measure_df, turbines, wtg_model_lookup)
        df2 = bucket_causes_gam(wtg_derate_df, wtg_measure_df, turbines, tag_lookup, site_setpoint_df, site)

        df1_ret = check_for_underperformance(df1, df2, turbines, power_gap_limit, min_ws_limit)

    elif (site in ["DAMA", "AMHST", "MOR"]):
        wtg_model_lookup = api.get_asset_model_lookup_at_site(site)
        wtg_tags: List[str] = [tag_lookup[site]["wtg_state"], tag_lookup[site]["wtg_power"], tag_lookup[site]["wtg_windspeed"], tag_lookup[site]["wtg_amb_temp"]]

        wtg_measure_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        
        site_setpoint_df =  api.get_measurement_timeseries([site], [tag_lookup[site]["site_setpoint"]], start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)

        df1 = prepare_raw_data(wtg_measure_df, turbines, wtg_model_lookup)
        df2 = bucket_causes_site_setpoint(wtg_measure_df, turbines, tag_lookup, site_setpoint_df, site)

        df1_ret = check_for_underperformance(df1, df2, turbines, power_gap_limit, min_ws_limit)
    ret_obj = {}
    for asset in turbines:
        num_condition_passes = df1_ret[f"v_"+asset+"-CONDITION_OK"].sum()
        num_flags = df1_ret[f"v_"+asset+"-POWER_GAP_FLAG"].sum() 
        if ((num_condition_passes > min_condition_passes) and (num_flags > 0) and (num_flags/num_condition_passes > min_condition_ratio)):
            print("Underperformance: ", asset, df1_ret[f"v_"+asset+"-CONDITION_OK"].sum(), df1_ret[f"v_"+asset+"-POWER_GAP_FLAG"].sum(), df1_ret[f"v_"+asset+"-POWER_GAP_VAL"].sum()/6/1000 )
            ret.append(
                {
                    "asset": asset,
                    "priority": "Monitor",
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "evidence": [{
                        "timestamp": int(api.datetime_to_unix_time(start_time[0])),
                        "variable": f"Condition Passed {df1_ret['v_'+asset+'-CONDITION_OK'].sum()} and caught {df1_ret['v_'+asset+'-POWER_GAP_FLAG'].sum()}",
                        "value": df1_ret[f"v_"+asset+"-POWER_GAP_FLAG"].sum()/df1_ret[f"v_"+asset+"-CONDITION_OK"].sum(),
                        "link": api.generate_power_curve_link_from_evidence(api.datetime_to_unix_time(start_time[0]), api.datetime_to_unix_time(end_time[0]), asset),  # noqa: E501
                        }],
                    "estimated_occurrence_loss": df1_ret[f"v_"+asset+"-POWER_GAP_VAL"].sum()/6/1000,
                    "estimated_aep_loss": 0,
                    "estimated_life_reduction_days": 0,
                    "status": "Office",
                }
            )
        elif (df1_ret[f"v_"+asset+"-CONDITION_OK"].sum() > 0):
            print("Marked but not past limit", asset, df1_ret[f"v_"+asset+"-CONDITION_OK"].sum(), df1_ret[f"v_"+asset+"-POWER_GAP_FLAG"].sum(), df1_ret[f"v_"+asset+"-POWER_GAP_VAL"].sum()/6/1000 )
        else: 
            print("ALL ZERO", asset, df1_ret[f"v_"+asset+"-CONDITION_OK"].sum(), df1_ret[f"v_"+asset+"-POWER_GAP_FLAG"].sum(), df1_ret[f"v_"+asset+"-POWER_GAP_VAL"].sum()/6/1000 )
        ret_obj[asset] = {"sumConditionOk": num_condition_passes, "sumPowerGapFlag": num_flags, "sumPowerGapValues":  df1_ret[f"v_"+asset+"-POWER_GAP_VAL"].sum()/6/1000}
    dfx = pd.DataFrame(ret_obj)
    # dfx.to_csv("./"+site+"_summaryFlags2.csv")
    # df1_ret.to_csv("./"+site+"_csvdumpflags.csv")
    return ret
