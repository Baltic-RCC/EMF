import pandas as pd
from datetime import datetime
from isodate import parse_duration
import logging
import config
import json
from dateutil import parser
from pathlib import Path
from emf.common.integrations.object_storage.models import query_data, get_content
from emf.common.integrations.minio_api import *
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)

parse_app_properties(caller_globals=globals(), path=config.paths.cgm_worker.replacement)


def run_replacement(tso_list: list,
                    time_horizon: str,
                    scenario_date: str,
                    config: list = json.load(config.paths.cgm_worker.replacement_conf),
                    data_source: str = 'OPDM',
                    ):
    """
     Args:
         tso_list: a list of tso's which models are missing models
         time_horizon: time_horizon of the merging process
         scenario_date: scenario_date of the merging process
         config: model replacement logic configuration
         data_source: model provision source type

     Returns:  from configuration a list of replaced models
    """
    replacement_models = []
    replacements = pd.DataFrame()
    # TODO time horizon exclusion logic + exclude available models from query
    # TODO put in query object type if CGM metadata objects will be stored
    query = {"pmd:TSO.keyword": tso_list, "valid": True, "data-source": data_source}
    body = query_data(query, QUERY_FILTER)
    model_df = pd.DataFrame(body)

    # Set scenario dat to UTC
    if not model_df.empty:
        scenario_date = parser.parse(scenario_date).strftime("%Y-%m-%dT%H:%M:%SZ")
        replacement_df = create_replacement_table(scenario_date, time_horizon, model_df, config)
        if not replacement_df.empty:
            unique_tsos_list = replacement_df["pmd:TSO"].unique().tolist()
            for unique_tso in unique_tsos_list:
                sample_tso = replacement_df.loc[(replacement_df["pmd:TSO"] == unique_tso)]
                sample_tso = sample_tso.loc[(sample_tso["priority_day"] == sample_tso["priority_day"].min())]
                sample_tso = sample_tso.loc[(sample_tso["priority_business"] == sample_tso["priority_business"].min())]
                sample_tso = sample_tso.loc[(sample_tso["priority_hour"] == sample_tso["priority_hour"].min())]
                sample_tso = sample_tso.loc[(sample_tso["pmd:versionNumber"] == sample_tso["pmd:versionNumber"].max())]
                sample_tso_min = sample_tso.loc[(sample_tso["pmd:creationDate"] == sample_tso["pmd:creationDate"].max())]
                if len(sample_tso_min) > 1:
                    logger.warning(f"Replacement filtering unreliable for: '{unique_tso}'")
                    sample_tso_min = sample_tso_min.iloc[:1]
                replacements = pd.concat([replacements, sample_tso_min])

            replacement_models = replacements.to_dict(orient='records') if not replacements.empty else None
            for num, model in enumerate(replacement_models):
                replacement_models[num] = get_content(model)

            replaced_tso = replacements['pmd:TSO'].unique().tolist()
            not_replaced = [model for model in unique_tsos_list if model not in replaced_tso]
            if not_replaced:
                logger.error(f"Unable to find replacements within given replacement logic for TSO's: {not_replaced}")

            tso_missing = [model for model in tso_list if model not in unique_tsos_list]
            if tso_missing:
                logger.info(f"No replacement models found for TSO(s): {tso_missing}")
        else:
            logger.error(f"No replacement models found, replacement list is empty")
    else:
        logger.info(f"No replacement models found in Elastic for TSO(s): {tso_list}")

    return replacement_models


# TODO deprecated, move to backlog
def run_replacement_local(tso_list: list,
                          time_horizon: str,
                          scenario_date: str,
                          config: list = json.load(config.paths.task_generator.timeframe_conf),
                          ):
    """
        Args:
            tso_list: a list of tso's which models are missing models
            time_horizon: time_horizon of the merging process
            scenario_date: scenario_date of the merging process
            config: model replacement logic configuration

        Returns:  from configuration a list of replaced models
       """
    replacement_models = []
    replacements = pd.DataFrame()
    # TODO time horizon exclusion logic + exclude available models from query
    client = ObjectStorage()
    list_elements = client.get_all_objects_name(bucket_name='opde-confidential-models', prefix='IGM')
    model_df=pd.DataFrame([item.split('-') for item in list_elements], columns=["pmd:scenarioDate","pmd:timeHorizon", "pmd:TSO", "pmd:versionNumber" ])
    model_df["pmd:creationDate"] = datetime.now()
    model_df["pmd:fileName"] = ['IGM/' + item for item in list_elements]
    model_df["pmd:TSO"] = model_df["pmd:TSO"]
    # print(model_df)
    # Set scenario dat to UTC
    if not model_df.empty:
        model_df["pmd:versionNumber"] = model_df["pmd:versionNumber"].apply(lambda x: x.split('.')[0])
        scenario_date = parser.parse(scenario_date).strftime("%Y-%m-%dT%H:%M:%SZ")
        replacement_df = create_replacement_table(scenario_date, time_horizon, model_df, config)
        if not replacement_df.empty:
            unique_tsos_list = tso_list

            for unique_tso in unique_tsos_list:
                sample_tso = replacement_df.loc[(replacement_df["pmd:TSO"] == unique_tso)]
                sample_tso = sample_tso.loc[(sample_tso["priority_day"] == sample_tso["priority_day"].min())]
                sample_tso = sample_tso.loc[(sample_tso["priority_business"] == sample_tso["priority_business"].min())]
                sample_tso = sample_tso.loc[(sample_tso["priority_hour"] == sample_tso["priority_hour"].min())]
                sample_tso = sample_tso.loc[(sample_tso["pmd:versionNumber"] == sample_tso["pmd:versionNumber"].max())]
                sample_tso_min = sample_tso.loc[(sample_tso["pmd:creationDate"] == sample_tso["pmd:creationDate"].max())]
                if len(sample_tso_min) > 1:
                    logger.warning(f"Replacement filtering unreliable for: '{unique_tso}'")
                    sample_tso_min = sample_tso_min.iloc[:1]
                replacements = pd.concat([replacements, sample_tso_min])

            replacement_models = replacements.to_dict(orient='records') if not replacements.empty else None
            if replacement_models:
                for num, model in enumerate(replacement_models):
                    replacement_models[num] = model

    return replacement_models


def make_lists_priority(timestamp, target_timehorizon, conf):
    """
     Args:
         timestamp: target timestamps where the hour conf is read from
         conf: main conf imported there the replacement dif timestamps are extracted

     Returns: from configuration a list of to be matched values
    """
    date_time = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    filter_hour = date_time.strftime("%H:%M")
    filter_day = date_time.weekday()
    hour_list = []
    day_list = []

    for hour in conf["hours"]:
        if hour["hour"] == filter_hour:
            hour_list = [item[key] for item in hour["priority"] for key in item]
    for day in conf["days"]:
        if day["day"] == filter_day:
            day_list = [item[key] for item in day["priority"] for key in item]

    hour_list_final = list(map(lambda x: (date_time + parse_duration(x)).strftime("%H:%M"), hour_list))
    day_list_final = list(map(lambda x: (date_time + parse_duration(x)).strftime("%Y-%m-%d"), day_list))

    business_list = conf["timeHorizon"]["Request_list"]
    business_list_final = business_list[business_list.index(target_timehorizon):]  # make list of relevant businesstypes

    # Month ahead requires separate replacement logic
    if target_timehorizon == 'MO':
        hour_list_final = [hour for hour in conf["month_ahead"]["hours"]]
        day_list_final = [get_first_monday_of_last_month(timestamp).strftime("%Y-%m-%d")]
        business_list_final = conf["month_ahead"]['business_type']

    return hour_list_final, day_list_final, business_list_final


def create_replacement_table(target_timestamp, target_timehorizon, valid_models_df, conf):
    """

    Args:
        target_timestamp: target_timestamp
        target_timehorizon: target_timehorizon
        valid_models_df: valid_models_df
        conf: conf

    Returns: replacement table with priorities for the matching timestamps

    """
    list_hour_priority, list_time_priority, list_business_priority = make_lists_priority(target_timestamp, target_timehorizon, conf) #make list of relevant Timestamps

    # Change ID naming for simpler replacement logic
    valid_models_df['pmd:timeHorizon'] = valid_models_df['pmd:timeHorizon'].apply(lambda x: 'ID' if x in [f'{i:02}' for i in range(1, 25)] else x)

    valid_models_df["priority_business"] = valid_models_df["pmd:timeHorizon"].apply(lambda x: list_business_priority.index(x) if x in list_business_priority else None)
    valid_models_df["pmd:scenarioDate"] = valid_models_df["pmd:scenarioDate"].apply(lambda x: parser.parse(x).strftime("%Y-%m-%dT%H:%M:%SZ"))
    valid_models_df["priority_hour"] = valid_models_df["pmd:scenarioDate"].apply(lambda x:
                                                                          list_hour_priority.index(datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").strftime("%H:%M"))
                                                                          if datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").strftime("%H:%M") in list_hour_priority else None)
    valid_models_df["priority_day"] = valid_models_df["pmd:scenarioDate"].apply(lambda x:
                                                                          list_time_priority.index(datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d"))
                                                                          if datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d") in list_time_priority else None)
    valid_models_df = valid_models_df.dropna(subset=["priority_hour", "priority_day", "priority_business"])

    return valid_models_df


def get_available_tsos():
    query = {"opde:Object-Type": "IGM", "valid": True}
    body = query_data(query, QUERY_FILTER)
    key = 'pmd:TSO'
    return list({item[key] for item in body if key in item})


def get_first_monday_of_last_month(timestamp):
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    if dt.month == 1:
        prev_month = 12
        prev_year = dt.year -1
    else:
        prev_month = dt.month -1
        prev_year = dt.year
    try:
        previous_month_day = dt.replace(month=prev_month, year=prev_year)
    except ValueError:
        first_day_of_current_month = dt.replace(day=1)
        previous_month_day = first_day_of_current_month - timedelta(days=1)

    first_day_of_month = previous_month_day.replace(day=1)
    weekday = first_day_of_month.weekday()
    days_to_add = (0 - weekday) % 7
    first_monday = first_day_of_month + timedelta(days=days_to_add)

    return first_monday


if __name__ == "__main__":

    missing_tso = ['PSE', 'LITGRID', 'AST']

    test_time_horizon = "MO"
    test_scenario_date = "2025-03-12T09:30:00Z"
    # print('hello')
    response_list = run_replacement(missing_tso, test_time_horizon, test_scenario_date)
    print('')
