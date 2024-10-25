from datetime import datetime, timedelta
from typing import List

from digital_punchlist.bazefield_api import BazefieldAPI, OutputItem

TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S.0000000Z'


def run(
    api: BazefieldAPI,
    site: str,
    turbines: List[str],
    start_time: datetime,
    end_time: datetime,
) -> List[OutputItem]:
    ret: List[OutputItem] = []

    site_full_name = api.get_site_full_name_from_short_name(site)

    ensight_events: dict = api.get_ensight_events_for_wtg_list(
        turbines,
        site_full_name,
        (start_time[0]-timedelta(days=1)).strftime(TIMESTAMP_FORMAT),
        (end_time[0]+timedelta(days=1)).strftime(TIMESTAMP_FORMAT)
    )

    for asset in ensight_events:
        if (
            asset["hcCode"] == "HC_01_01_02_04"
            # asset["alarmCategory"] == "Health"
            # and asset["alarmTypeEn"] == "Generator"
            # and asset["alarmDescriptionEn"] == "Temperature anomaly on generator non-drive-end bearing and related components"  # noqa E501
        ):
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
            ret.append(
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
                        "variable": "Ensight: Health",
                        "value": -1*asset['aepLoss'],
                        "link":  api.get_ensight_link_from_site_and_type(site, "health"),  # noqa: E501
                    }],
                }
            )

    return ret
