from datetime import datetime

import pandas as pd
import requests


def get_all_asset_priority_status():
    """
    This function requests the asset priority status for all the assets owned by liberty and returns a DataFrame of
    dictionaries which each contains the asset priority status of the turbine, here is an extract from the API
    documentation explaining the priority number:



    The current value for the requested asset(s) priority(ies) is an enumerated constant “priority” with values
    corresponding to the terms below:
        1. Okay
        2. Low
        3. Medium
        4. High
        5. Critical
        6. Pending review: awaiting assessment by monitoring team



    :return: A DataFrame of dictionaries which each contains the asset priority status of the turbine
    """
    url = 'https://psl2backend.poseidonsys.com/graphql'
    body = """query activeAssetsWithPriority($model: String, $modelId: ID) {
    activeAssetsWithPriority(model: $model, modelId: $modelId) {
    id
    name
    obfuscateName
    fullName
    priority
    priorityComponents {
    id
    name
    priority
    }
    missedCommunicationDevices {
    name
    }
    location {
    id
    name
    obfuscateName
    company {
    id
    name
    obfuscateName
    }
    }
    }
    }
    """

    vars = {
        "modelId": '6189537e01cafc001b11244f',
        "model": 'company',
    }

    response = requests.post(url=url, json={"query": body, "variables": vars},
                             auth=("", ""))
    if response.ok:
        response = pd.DataFrame(response.json()["data"]["activeAssetsWithPriority"])
        return response
    return None
