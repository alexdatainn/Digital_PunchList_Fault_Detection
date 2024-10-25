- finish and verify engine update for Auto-close, with blade load fault as example (if item previously existed/had been caught before by the engine, and the engine doesnt find it again when it runs this week, then change that "Open" status to "Auto-close") (~2.5h)
- Change "Stop Turbine" priority to "Stop: in backend and frontend (~1.5h)
- update all ensight plugins to use key "hcCode" to identify issue instead of text keys (~1.5h)
- verify ensight plugins working correctly by comparing to both csv export and bazefield UI (~0.5h)





























print("FALSE")
                    for att in new_items:
                        if att["name"] == "List of Occurances and Evidence":
                            att["value"] = item_prev_occ
                        elif att["name"] == "Action Events":
                            att["value"] = item_prev_close
                        elif att["name"] == "Status":
                            att["value"] = item_prev_status
                    post_data["columns"] = []
                    post_data["columns"] = json.dumps(
                        new_items, separators=(",", ":")
                    )
                    post_data["id"] = item["id"]
                    updated = True\
        



        for att in new_items:
                    if att["name"] == "List of Occurances and Evidence":
                        update_prev = 0
                        for prev in item_prev_occ:
                            if (
                                att["value"][0]['start'] == prev['start']
                                and att["value"][0]['end'] == prev['end']
                            ):
                                if 'lost_mwh' in att["value"][0]:
                                    prev['lost_mwh'] = att["value"][0]['lost_mwh']  # noqa E501
                                if 'value' in att["value"][0]:
                                    prev['value'] = att["value"][0]['value']
                                if 'ref_value' in att["value"][0]:
                                    prev['value'] = att["value"][0]['ref_value']  # noqa E501
                                if 'timestamp' in att["value"][0]:
                                    prev['timestamp'] = att["value"][0]['timestamp']  # noqa E501
                                if 'link' in att["value"][0]:
                                    prev['link'] = att["value"][0]['link']
                                if 'evidence' in att["value"][0]:
                                    prev["evidence"] = att["value"][0]["evidence"]  # noqa E501
                                update_prev = 1
                                break
                        if update_prev == 0:
                            att["value"] = item_prev_occ + att["value"]
                        else:
                            att["value"] = item_prev_occ
                    elif att["name"] == "Action Events":
                        att["value"] = item_prev_close + att["value"]
                    elif att["name"] == "Status":
                        att["value"] = item_prev_status
                post_data["columns"] = []
                post_data["columns"] = json.dumps(
                    new_items, separators=(",", ":")
                )
                post_data["id"] = item["id"]
                updated = True
                break