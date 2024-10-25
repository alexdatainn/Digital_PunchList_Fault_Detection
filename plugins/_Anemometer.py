import json
from datetime import datetime, timedelta
from typing import Dict, List

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
                    elif running==1 and ws_val > 0:
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
                    elif running==1 and ws_val > 0:
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
                    elif running==1 and ws_val > 0:
                        cause_df["v_" + wtg + "-BUCKETNUMBER"][timestamp] = 0
        return cause_df
    
    def check_for_anomaly(measures, df1, df2):
        occurances: Dict = {}
        good_assets: Dict = {}
        min_weekly_data_avail: Dict = {}
        loss_values: Dict = {}
        tot_act_pow: Dict = {}
        tot_fact_pow: Dict = {}
        binned_act_pow: Dict = {}
        binned_fact_pow: Dict = {}
        for wtg in turbines:
            occurances[wtg] = []
            good_assets[wtg] = []
            min_weekly_data_avail[wtg] = 0
            loss_values[wtg] = 0
            tot_act_pow[wtg] = 0
            tot_fact_pow[wtg] = 0
            binned_act_pow[wtg] = [0]*len(wtg_model_lookup[wtg]['lookupWindBin'])
            binned_fact_pow[wtg] = [0]*len(wtg_model_lookup[wtg]['lookupWindBin'])
            
        for tag in tags:
            full_tags: List[str] = []
            for turb1 in turbines:
                if ('v_' + turb1 + '-' + tag) in measures:
                    full_tags.append('v_' + turb1 + '-' + tag)

            median_series = pd.DataFrame(measures[full_tags].median(axis='columns'))  # noqa E501
            median_series.columns = ['median_' + tag]
            measures = measures.join(median_series)

            for turb in turbines:
                if (
                    (('v_' + turb + '-' + tag) in measures) and
                    (('v_' + turb + '-' + power_tags[0]) in df1) and
                    (('v_' + turb + '-' + state_tags[0]) in df1)
                ):
                    for ind, val in enumerate(measures['v_' + turb + '-' + tag]):
                        timestamp = measures['median_' + tag].index[ind]
                        median_val = measures['median_' + tag][timestamp]
                        power_val = df1['v_' + turb + '-' + power_tags[0]][timestamp]
                        state_val = df1['v_' + turb + '-' + state_tags[0]][timestamp]
                        bucket_val = df2["v_" + turb + "-BUCKETNUMBER"][timestamp]
                        if (
                            (state_val == 4)
                            and (power_val > 0)
                            and (median_val < max_ws) and (median_val > min_ws)
                            and (val < max_ws) and (val > min_ws)
                        ):
                            binned_ws = round(val*2, 0)/2
                            if (binned_ws > 0 and binned_ws in wtg_model_lookup[turb]['lookupWindBin']):
                                ws_ind = wtg_model_lookup[turb]['lookupWindBin'].index(binned_ws)
                            else:
                                ws_ind = 0
                            fact_pow = wtg_model_lookup[turb]['lookupPower'][ws_ind]
                            min_weekly_data_avail[turb] += 1
                            ws_diff_calc = (median_val-val)/median_val
                            if fact_pow > 0:
                                pow_diff_calc = (fact_pow-power_val)/fact_pow
                            else:
                                pow_diff_calc = 0
                            if (
                                (val > 4) and
                                (abs(ws_diff_calc) > max_ws_diff_percent) and
                                # (pow_diff_calc > max_power_diff_percent) and
                                (bucket_val == 0)
                                ):
                                if (pow_diff_calc > max_power_diff_percent):
                                    occurances[turb].append({
                                        "timestamp": timestamp,
                                        "value": val,
                                        "referenceValue": median_val,
                                        "factPower": fact_pow,
                                        "actualPower": power_val
                                        })
                                    loss_values[turb] += (fact_pow - power_val)
                                tot_act_pow[turb] += power_val
                                tot_fact_pow[turb] += fact_pow
                                binned_act_pow[turb][ws_ind] += power_val
                                binned_fact_pow[turb][ws_ind] += fact_pow

                            elif ((abs(ws_diff_calc)  < max_ws_diff_percent) and (pow_diff_calc  < max_power_diff_percent)):
                                good_assets[turb].append({
                                    "timestamp": timestamp,
                                    "value": val,
                                    "referenceValue": median_val,
                                    "factPower": fact_pow,
                                    "actualPower": power_val
                                    })
        return (occurances, good_assets, min_weekly_data_avail, loss_values, tot_act_pow, tot_fact_pow, binned_act_pow, binned_fact_pow)

    ret: List[OutputItem] = []
    interval_ms: int = 600000
    min_ws: int = 4
    max_ws: int = 9999
    max_ws_diff_percent = 0.15
    max_power_diff_percent = 0.10
    min_duration: float = 0.20*(end_time[0]-start_time[0])/timedelta(hours=1)
    min_good_duration: float = 0.25*(end_time[0]-start_time[0])/timedelta(hours=1)
    wtg_derate_tags: List[str] = []
    tags: List[str] = ["WindSpeed"]
    state_tags: List[str] = ["IEC-OperationState"]
    power_tags: List[str] = ["ActivePower"]
    wtg_model_lookup = api.get_asset_model_lookup_at_site(site)
    measures: pd.DataFrame = api.get_measurement_timeseries(
        turbines, tags, start_time[0], end_time[0], interval_ms,
        aggregate_type=AggregateType.time_average
        )
    with open('./configs/derate_tag_lookup.json', 'r') as f:
        tag_lookup = json.load(f)
        for der_key in tag_lookup[site]["derateKeys"]:
            wtg_derate_tags.append(tag_lookup[site][der_key])

    wtg_tags: List[str] = [tag_lookup[site]["wtg_state"], tag_lookup[site]["wtg_power"], tag_lookup[site]["wtg_windspeed"], tag_lookup[site]["wtg_amb_temp"]]
    wtg_measure_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
    
    df1 = prepare_raw_data(wtg_measure_df, turbines, wtg_model_lookup)

    if ((site == "MAV") or (site == "BLH")):
        if (site == "MAV"):
            turbines = ["MAV-WTGT101", "MAV-WTGT102", "MAV-WTGT103", "MAV-WTGT104", "MAV-WTGT105", "MAV-WTGT116", "MAV-WTGT141", "MAV-WTGT142", "MAV-WTGT143", "MAV-WTGT144", "MAV-WTGT145", "MAV-WTGT146", "MAV-WTGT147", "MAV-WTGT148", "MAV-WTGT149", "MAV-WTGT150"]
        wtg_derate_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_derate_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.bool)
        df2 = bucket_causes_sgre(wtg_derate_df, wtg_measure_df, turbines, tag_lookup, site)

    elif (site in ["OWF", "SUGR", "DFS"]):
        wtg_derate_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_derate_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        df2 = bucket_causes_vestas(wtg_derate_df, wtg_measure_df, turbines, tag_lookup, site)

    elif (site in ["SENT", "SNDY", "MN"]):
        wtg_derate_df: pd.DataFrame = api.get_measurement_timeseries(turbines, wtg_derate_tags, start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.bool)
        site_setpoint_df =  api.get_measurement_timeseries([site], [tag_lookup[site]["site_setpoint"]], start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        df2 = bucket_causes_gam(wtg_derate_df, wtg_measure_df, turbines, tag_lookup, site_setpoint_df, site)

    elif (site in ["DAMA", "AMHST", "MOR"]):
        site_setpoint_df =  api.get_measurement_timeseries([site], [tag_lookup[site]["site_setpoint"]], start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        df2 = bucket_causes_site_setpoint(wtg_measure_df, turbines, tag_lookup, site_setpoint_df, site)
    else:
        site_setpoint_df =  api.get_measurement_timeseries([site], [tag_lookup[site]["site_setpoint"]], start_time[0], end_time[0], interval_ms=interval_ms, aggregate_type=AggregateType.time_average)
        df2 = bucket_causes_site_setpoint(wtg_measure_df, turbines, tag_lookup, site_setpoint_df, site)

    
    

    (occurances, good_assets, min_weekly_data_avail, loss_values, tot_act_pow, tot_fact_pow, binned_act_pow, binned_fact_pow) = check_for_anomaly(measures, df1, df2)

    for asset in occurances:
        num_hours = len(occurances[asset])/6
        if (min_weekly_data_avail[asset]/6 > min_good_duration) and (len(occurances[asset])/6 > min_duration):
            link_assets = [asset + '-' + tags[0]]
            for neighbour_asset in good_assets:
                if len(good_assets[neighbour_asset])/6 > min_good_duration:
                    link_assets = [asset + '-' + tags[0], neighbour_asset + '-' + tags[0]]
                    break
            tot_pow_diff = round(tot_fact_pow[asset]-tot_act_pow[asset])
            pow_act = round(tot_act_pow[asset])
            pow_fact = round(tot_fact_pow[asset])
            pow_loss = round(loss_values[asset])
            binned_act_str = str(binned_act_pow[asset])
            binned_fact_str = str(binned_fact_pow[asset])
            wind_bins = str(wtg_model_lookup[asset]['lookupWindBin'])

            ret.append(
                {
                    "asset": asset,
                    "priority": "Next PM",
                    "certainty": "Monitor",
                    "occurrence_start": start_time[0],
                    "occurrence_end": end_time[0],
                    "evidence": [{
                        "timestamp": int(occurances[asset][0]["timestamp"]),
                        "variable": f"num datapoints: {min_weekly_data_avail[asset]}, {asset}-{tags[0]} num hrs dev: {num_hours}, total_act_pow: {pow_act}, total_fact_pow: {pow_fact}, total_diff: {tot_pow_diff}, total_loss_when_10percentunder: {pow_loss}, binned_act_pow: {binned_act_str}, binned_fact_pow: {binned_fact_str}, wind_bins: {wind_bins}",
                        "value": len(occurances[asset])/6,
                        "link": api.generate_trend_link_from_evidence_using_start_and_end(link_assets, api.datetime_to_unix_time(start_time[0]), api.datetime_to_unix_time(end_time[0])),
                    }],
                    "estimated_occurrence_loss": round(loss_values[asset]/6/1000, 1),
                    "estimated_aep_loss": 36,
                    "estimated_life_reduction_days": 0,
                    "status": "Office",
                }
            )

    return ret
