from core import query_data

def get_latest_boundary():
    """
    Asks latest boundary entry from elastic and fetches the corresponding files from minio.
    Returns something if all files are present (note the expiry date in minio)
    1) gets by opde:Object-Type: "BDS"
    2) Sorts the results by pmd:scenarioDate
    3) takes the latest entry
    4) fetches minio models
    """
    bds_query = {"opde:Object-Type.keyword": "BDS"}
    sort_by_date = {"pmd:scenarioDate": {"order": "desc"}}
    # TODO - sort by version

    query_response = query_data(metadata_query=bds_query, sort=sort_by_date, return_payload=True, size='1')

    if latest_boundary := (query_response[0]):
        # Check if all files are present, if are then return if not then return None and catch it later
        if all(profile.get("opdm:Profile", {}).get("DATA")
               for profile in dict(latest_boundary).get("opde:Component", [])):
            return latest_boundary
    return None

def get_models(time_horizon: str, scenario_date: str, valid=True):
    """
    Asks metadata from elastic, attaches files from minio
    NB! currently only those models are returned which have files in minio

    :param time_horizon: the time horizon
    :param scenario_date: the date requested
    :return: list of models
    """
    query = {'pmd:scenarioDate': scenario_date,
             'pmd:timeHorizon': time_horizon,
             'valid': valid}

    query_response = query_data(metadata_query=query, return_payload=True)

    # Remove models that are missing data
    files_present = [model for model in query_response if all(field.get('opdm:Profile', {}).get("DATA") for field in model.get('opde:Component', {}))]
    query_response = files_present


    return query_response