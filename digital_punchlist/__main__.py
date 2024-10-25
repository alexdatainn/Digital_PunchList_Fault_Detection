import csv
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from digital_punchlist.bazefield_api import BazefieldAPI, Config, OutputItem
from digital_punchlist.plugin import PluginManager

# from dotenv import dotenv_values


PLUGINS_FOLDER: str = "plugins"
TIMESTAMP_FORMAT: str = '%Y-%m-%dT%H:%M:%S.0000000Z'


def _get_delta_from_frequency(frequency: str) -> timedelta:
    suffix: str = frequency[-1]
    value: int = int(frequency[:-1])
    return {
        "w": timedelta(weeks=value),
        "d": timedelta(days=value),
        "h": timedelta(hours=value),
    }.get(suffix, timedelta())


def _plugin_should_be_run(
    last_run: datetime, now: datetime, delta: timedelta
) -> bool:
    return last_run + delta <= now


def _run_plugin_for_site(
    plugin_manager: PluginManager,
    bf_api: BazefieldAPI,
    criteria_code: str,
    site: str,
    turbines: List[str],
    assesment_frequencies: Dict[str, str],
    now: datetime,
    offset: int,
    start_date: datetime
) -> List[OutputItem]:
    logging.debug(
        f"Running '{criteria_code}' for site '{site}' with turbines \"{' '.join(turbines)}\"."  # noqa: E501
    )

    logging.debug(
        f"Running {PLUGINS_FOLDER}/{criteria_code}.py for site {site}"
    )

    start_times: List[datetime] = []
    end_times: List[datetime] = []

    for turbine in turbines:
        # check_freq: str = assesment_frequencies[turbine]
        check_freq = '7d'
        if (start_date == now):
            delta = _get_delta_from_frequency(check_freq)
            end = datetime(now.year, now.month, now.day)
            start = end - delta
            use_freq = True
        else:
            delta = _get_delta_from_frequency(check_freq)
            end = datetime(now.year, now.month, now.day)  - timedelta(days=7*offset)
            start = datetime(start_date.year, start_date.month, start_date.day)  - timedelta(days=7*offset)# noqa E501
            use_freq = False

        if use_freq and (check_freq == "30d" or check_freq == "1m"):
            end = now - timedelta(
                hours=now.hour,
                minutes=now.minute,
                seconds=now.second,
            ) - timedelta(days=offset * 30)
            start = end - timedelta(days=30)
        elif use_freq and check_freq == "7d" or check_freq == "1w":
            end = now - timedelta(
                days=now.weekday(),
                hours=now.hour,
                minutes=now.minute,
                seconds=now.second,
            ) - timedelta(weeks=offset)
            start = end - timedelta(weeks=1)
        elif use_freq and check_freq == "24h" or check_freq == "1d":
            end = now - timedelta(
                hours=now.hour,
                minutes=now.minute,
                seconds=now.second,
            ) - timedelta(days=offset)
            start = end - timedelta(days=1)

        elif use_freq and check_freq == "1h":
            end = now - timedelta(
                minutes=now.minute,
                seconds=now.second,
            ) - timedelta(hours=offset)
            start = end - timedelta(hours=1)

        start_times.append(start)
        end_times.append(end)
    logging.info(
        f"Running '{criteria_code}' for site '{site}' . Week Starting '{start_times[0]}' and Ending={end_times[0]} (offset={offset})' ."  # noqa: E501

    )
    return plugin_manager.run_module(
        criteria_code, bf_api, site, turbines, start_times, end_times
    )


def main():
    """
    Run Modes:

    Normal Engine Runtime: python -m digital_punchlist "ENGINE"

    Developer Normal Runtime:
    python -m digital_punchlist "DEVELOPER" "[insert_name_of_config].json"

    Developer Runtime to List Ensight Events @ sites/start/end in config:
    python -m digital_punchlist "DEVELOPER_ENSIGHT" "[insert_name_of_config].json"  # noqa E501



    if len(sys.argv) == 3:
        RUNTIME_MODE = sys.argv[1]
        CONFIG_FILENAME = sys.argv[2]
    elif len(sys.argv) == 2:
        RUNTIME_MODE = sys.argv[1]
        CONFIG_FILENAME = "developerConfig.json"
    else:
        RUNTIME_MODE = "ENGINE"  # "DEVELOPER"  # "ENGINE"
        CONFIG_FILENAME = "developerConfig.json"
    # env: dict[str, str] = dotenv_values(".env")
    """
    RUNTIME_MODE = "ENGINE"  # "DEVELOPER"  # "ENGINE"
    CONFIG_FILENAME = "developerConfig.json"

    head: int = 0
    ACCESS_TOKEN: Optional[str] = ""
    if ACCESS_TOKEN is None:
        logging.error("Access token was not set in .env, exiting")
        sys.exit(1)
    ENSIGHT_ACCESS_KEY: Optional[str] = ""  # env["EN_ACCESS_KEY"]
    if ENSIGHT_ACCESS_KEY is None:
        logging.error("Ensight Access key was not set in .env, exiting")
        sys.exit(1)
    ENSIGHT_SECRET_KEY: Optional[str] = ""  # env["EN_SECRET_KEY"]
    if ENSIGHT_SECRET_KEY is None:
        logging.error("Ensight Secret key was not set in .env, exiting")
        sys.exit(1)

    bf_api: BazefieldAPI = BazefieldAPI(
        ACCESS_TOKEN, ENSIGHT_ACCESS_KEY, ENSIGHT_SECRET_KEY
    )
    plugin_manager: PluginManager = PluginManager()
    plugin_manager.load_plugins(PLUGINS_FOLDER)

    last_runs: Dict[str, Dict[str, Dict[str, int]]] = {}
    if os.path.exists(".lastruns"):
        with open(".lastruns") as last_runs_file:
            last_runs = json.loads(last_runs_file.read())

    json_config: Dict
    if os.path.exists(CONFIG_FILENAME):
        with open(CONFIG_FILENAME) as json_config_file:
            json_config = json.loads(json_config_file.read())

    now: datetime = datetime(
        datetime.now().year,
        datetime.now().month,
        datetime.now().day,
        datetime.now().hour,
        datetime.now().minute,
        datetime.now().second
    )
    turbines_to_run: Dict[str, Dict[str, List[str]]] = {}
    asset_check_freq: Dict[str, Dict[str, Dict[str, str]]] = {}
    head = 0
    if (RUNTIME_MODE == "ENGINE"):
        CSV_ARCHIVE_FILENAME = "output_files/DB_ARCHIVE_"+datetime.strftime(datetime.now(),"%Y%m%d%H%M%S")+'.csv' # noqa E501
        with open(CSV_ARCHIVE_FILENAME, 'a', newline='') as csv_out:
            csv_writer = csv.writer(csv_out)
            fleet_config: List[Config] = bf_api.get_fleet_configuration()
            for config in fleet_config:
                criteria_code: str = config["criteria_code"]
                site: str = config["site"]
                turbine: str = config["wtg"]
                check_freq: str = config["assesment_frequency"]
                is_enabled: bool = config["enabled"]
                if not is_enabled:
                    continue
                delta: timedelta = _get_delta_from_frequency(check_freq)  # noqa: E501

                if criteria_code not in last_runs:
                    last_runs[criteria_code] = {}

                if site not in last_runs[criteria_code]:
                    last_runs[criteria_code][site] = {}

                last_run: datetime = datetime.fromtimestamp(
                    last_runs[criteria_code][site].get(turbine, 0)
                )

                if not _plugin_should_be_run(last_run, now, delta):
                    logging.info(
                        f"Not running '{criteria_code}' for turbine '{turbine}' on '{site}' because {check_freq} has not passed."  # noqa: E501
                    )
                    continue

                if criteria_code not in turbines_to_run:
                    turbines_to_run[criteria_code] = {}
                    asset_check_freq[criteria_code] = {}

                if site not in turbines_to_run[criteria_code]:
                    turbines_to_run[criteria_code][site] = []
                    asset_check_freq[criteria_code][site] = {}

                turbines_to_run[criteria_code][site].append(turbine)
                asset_check_freq[criteria_code][site][turbine] = check_freq
            numIntervalsRun: int = 1  # here
            for offset in range(numIntervalsRun):
                offset_to_run = numIntervalsRun - offset - 1
                logging.info(f"Running for offset_to_run: '{offset_to_run}'")
                for criteria_code in turbines_to_run:
                    if criteria_code in ["ALARMS-P001", "ALARMS-P002", "ALARMS-P003", "ALARMS-P004", "ALARMS-P006", "ENSIGHT-P801", "ENSIGHT-P802", "ENSIGHT-P803", "ENSIGHT-P806", "ENSIGHT-P807", "ENSIGHT-P808", "ENSIGHT-P809", "ENSIGHT-P811", "ENSIGHT-P851", "ENSIGHT-P901", "ENSIGHT-P902","ENSIGHT-P903", "ENSIGHT-P904", "ENSIGHT-P951","ENSIGHT-P952","ENSIGHT-P953", "ENSIGHT-P954", "MIXED-P602", "MIXED-P603", "MIXED-P604", "MIXED-P605", "MIXED-P606", "MIXED-P700", "TREND-P401", "TREND-P402" "TREND-P404", "TREND-P410", "TREND-P412", "MIXED-P650", "MIXED-P651"]:  # noqa E501
                        for site in turbines_to_run[criteria_code]:
                            turbines = turbines_to_run[criteria_code][site]
                            if len(turbines) == 0:
                                continue
                            assesment_frequencies = asset_check_freq[criteria_code][site]  # noqa E501

                            for turbine in turbines:
                                last_runs[criteria_code][site][turbine] = int(
                                    round(now.timestamp())
                                )
                            plugin_ret: List[OutputItem] = _run_plugin_for_site(
                                plugin_manager, bf_api, criteria_code, site,
                                turbines, assesment_frequencies, now,
                                offset_to_run, now
                            )
                            if plugin_ret is None:
                                continue
                            if head == 0:
                                csv_writer.writerow([
                                    "Site", "WTG", "Criteria Code",
                                    "Criteria Revision", "Priority", "Certainty",
                                    "List of Occurances and Evidence",
                                    "Sum Measured Loss (MWh)",
                                    "Estimated AEP Loss (MWh)",
                                    "Estimated Life Reduction (Days)",
                                    "Status", "Action Events",
                                ])
                                head += 1
                            for item in plugin_ret:
                                bf_api.update_output_item(
                                    site, item["asset"], criteria_code,
                                    item, csv_writer, post_enabled=True)

    elif (RUNTIME_MODE == "DEVELOPER"):
        CSV_ARCHIVE_FILENAME = "output_files/"+json_config["csvOutputFilename"]+"_"+datetime.strftime(datetime.now(),"%Y%m%d%H%M%S")+'.csv' # noqa E501
        start_local = datetime.strptime(json_config["startDateYYYYmmdd"], "%Y%m%d")  # noqa E501
        end_local = datetime.strptime(json_config["endDateYYYYmmdd"], "%Y%m%d")
        with open(CSV_ARCHIVE_FILENAME, 'a', newline='') as csv_out:
            csv_writer = csv.writer(csv_out)
            fleet_config: List[Config] = []
            for cfg_plugin in list(json_config["listPluginsToTest"]):
                for cfg_site in list(json_config["listSitesToInclude"]):
                    list_cfg_wtgs = bf_api.get_asset_names_at_site(cfg_site)
                    for cfg_asset in list_cfg_wtgs:
                        if cfg_asset not in json_config["excludeAssetsIfAny"]:
                            fleet_config.append({
                                'criteria_code': cfg_plugin,
                                'site': cfg_site,
                                "wtg": cfg_asset,
                                "assesment_frequency": '7d',
                                "enabled": True,
                                "note": 'dev_test'
                            })
            for config in fleet_config:
                criteria_code: str = config["criteria_code"]
                site: str = config["site"]
                if (
                    (criteria_code in json_config['listPluginsToTest'])
                    and (site in json_config['listSitesToInclude'])
                ):
                    turbine: str = config["wtg"]
                    check_freq: str = config["assesment_frequency"]
                    is_enabled: bool = config["enabled"]

                    if not is_enabled:
                        continue

                    delta: timedelta = _get_delta_from_frequency(check_freq)  # noqa: E501

                    if criteria_code not in last_runs:
                        last_runs[criteria_code] = {}

                    if site not in last_runs[criteria_code]:
                        last_runs[criteria_code][site] = {}

                    last_run: datetime = datetime.fromtimestamp(
                        last_runs[criteria_code][site].get(turbine, 0)
                    )

                    if not _plugin_should_be_run(last_run, now, delta):
                        logging.info(
                            f"Not running '{criteria_code}' for turbine '{turbine}' on '{site}' because {check_freq} has not passed."  # noqa: E501
                        )
                        continue

                    if criteria_code not in turbines_to_run:
                        turbines_to_run[criteria_code] = {}
                        asset_check_freq[criteria_code] = {}

                    if site not in turbines_to_run[criteria_code]:
                        turbines_to_run[criteria_code][site] = []
                        asset_check_freq[criteria_code][site] = {}

                    turbines_to_run[criteria_code][site].append(turbine)
                    asset_check_freq[criteria_code][site][
                        turbine
                    ] = check_freq
            numIntervalsRun: int = 1  # here
            for offset in range(numIntervalsRun):
                head = 0
                offset_to_run = numIntervalsRun - offset - 1
                logging.info(f"Running for offset_to_run: '{offset_to_run}'")
                for criteria_code in turbines_to_run:
                    for site in turbines_to_run[criteria_code]:
                        turbines = turbines_to_run[criteria_code][site]

                        if len(turbines) == 0:
                            continue

                        assesment_frequencies = asset_check_freq[criteria_code][site]  # noqa E501

                        for turbine in turbines:
                            last_runs[criteria_code][site][turbine] = int(
                                round(now.timestamp())
                            )

                        plugin_ret: List[OutputItem] = _run_plugin_for_site(
                            plugin_manager, bf_api, criteria_code, site,
                            turbines, assesment_frequencies, end_local,
                            offset_to_run, start_local
                        )

                        if plugin_ret is None:
                            continue
                        if head == 0:
                            csv_writer.writerow([
                                "Site", "WTG", "Criteria Code",
                                "Criteria Revision", "Priority", "Certainty",
                                "List of Occurances and Evidence",
                                "Sum Measured Loss (MWh)",
                                "Estimated AEP Loss (MWh)",
                                "Estimated Life Reduction (Days)",
                                "Status", "Action Events",
                            ])
                            head += 1
                        for item in plugin_ret:
                            bf_api.update_output_item(
                                site, item["asset"], criteria_code,
                                item, csv_writer, post_enabled=False)
    elif (RUNTIME_MODE == "DEVELOPER_ENSIGHT"):
        CSV_ARCHIVE_FILENAME = "output_files/"+"ENSIGHT_EVENTS_"+datetime.strftime(datetime.now(),"%Y%m%d%H%M%S")+'.csv' # noqa E501
        start_local = datetime.strptime(json_config["startDateYYYYmmdd"], "%Y%m%d")  # noqa E501
        end_local = datetime.strptime(json_config["endDateYYYYmmdd"], "%Y%m%d")
        ensight_event_list = []
        fleet_config: List[Config] = []
        for cfg_site in list(json_config["listSitesToInclude"]):
            site_full_name = bf_api.get_site_full_name_from_short_name(cfg_site)  # noqa E501
            list_cfg_wtgs = bf_api.get_asset_names_at_site(cfg_site)
            ensight_events = bf_api.get_ensight_events_for_wtg_list(
                list_cfg_wtgs, site_full_name,
                (start_local).strftime(TIMESTAMP_FORMAT),
                (end_local).strftime(TIMESTAMP_FORMAT)
            )
            ensight_event_list = ensight_event_list + ensight_events
            print(ensight_events)
        ret_df = pd.DataFrame(ensight_event_list)
        ret_df.to_csv(CSV_ARCHIVE_FILENAME)
        print('')


if __name__ == "__main__":
    FMT_STR: str = "(%(asctime)s | %(levelname)s) [%(filename)s:%(lineno)s] %(message)s"  # noqa: E501
    run_start_time = datetime.now()
    date_str = run_start_time.strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(
        level=logging.INFO,
        format=FMT_STR,
        handlers=[logging.FileHandler("log_files/log_"+date_str), logging.StreamHandler()],  # noqa: E501
    )
    logging.info("Checking if plugins need to be run.")
    main()
    run_end_time = datetime.now()
    logging.info("Completed in " + str(run_end_time-run_start_time))
    print('')
