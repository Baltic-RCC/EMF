import pandas as pd
from datetime import datetime
from isodate import parse_duration


#SOLUTION 2- add priotities
def make_lists_priority(timestamp,conf):
    """
     Args:
         timestamp: target timestamps where the hour conf is read from
         conf: main conf imported there the replacement dif timestamps are extracted

     Returns: from configuration a list of to be matched values
    """
    date_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    filter_hour = date_time.strftime("%H:%M")
    filter_day = date_time.weekday() + 1

    for hour in conf["hours"]:
        if hour["hour"] == filter_hour:
            hour_list = [item[key] for item in hour["priority"] for key in item]
    for day in conf["days"]:
        if day["day"] == filter_day:
            day_list = [item[key] for item in day["priority"] for key in item]

    hour_list_final = list(map(lambda x: (date_time + parse_duration(x)).strftime("%H:%M"),hour_list))
    day_list_final = list(map(lambda x: (date_time + parse_duration(x)).strftime("%Y-%m-%d"),day_list))

    business_list = conf["timeHorizon"]["Request_list"]
    business_list_final = business_list[business_list.index(target_timehorizon):]  # make list of relevant businesstypes
    return hour_list_final, day_list_final, business_list_final

def create_current_replacement_table2(target_timestamp, target_timehorizon, valid_models_df, conf):
    """

    Args:
        target_timestamp: target_timestamp
        target_timehorizon: target_timehorizon
        valid_models_df: valid_models_df
        conf: conf

    Returns: replacement table with priorities for the matching timestamps

    """
    list_hour_priority, list_time_priority,list_business_priority  = make_lists_priority(target_timestamp, conf) #make list of relevant Timestamps

    valid_models_df["priority_business"] = valid_models_df["time_horison"].apply(lambda x: list_business_priority.index(x) if x in list_business_priority else None)
    valid_models_df["priority_hour"] = valid_models_df["timestamp"].apply(lambda x:
                                                                          list_hour_priority.index(datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%H:%M"))
                                                                          if datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%H:%M") in list_hour_priority else None)
    valid_models_df["priority_day"] = valid_models_df["timestamp"].apply(lambda x:
                                                                          list_time_priority.index(datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d"))
                                                                          if datetime.strptime(x, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d") in list_time_priority else None)
    #print(valid_models_df)
    valid_models_df = valid_models_df.dropna(subset=["priority_hour", "priority_day", "priority_business"])

    return valid_models_df



if __name__ == "__main__":

    ## TO DO:
    #1. build elastic query to retrive the valid_model_df for input

    #TEST DATASET: CONF, MODEL_DATA
    conf = {
        "configuration": {
            "validity_start": "2024-02-22T00:00:00Z",
            "validity_end": "2024-02-22T23:59:59Z",
            "timeZone": "CET"
        },
        "timeHorizon": {
            "Request_list": ["ID", "1D", "2D", "WK"]
        },
        "hours": [
            {
                "hour": "00:30",
                "priority": [
                    {1: "PT0H"},
                    {2: "PT1H"},
                    {3: "-PT1H"},
                    {4: "-PT2H"}
                ]
            },
            {
                "hour": "01:30",
                "priority": [
                    {1: "PT0H"},
                    {2: "PT1H"},
                    {3: "-PT1H"},
                    {4: "-PT2H"}
                ]
            }
        ],
        "days": [
            {
                "day": 0,
                "priority": [
                    {1: "P0D"},
                    {2: "P1D"}
                ]
            },
            {
                "day": 1,
                "priority": [
                    {1: "P0D"},
                    {2: "P1D"}
                ]
            }
        ]
    }
    test_model_data = {
        "name": ["elering", "ast", "litgrid","elering", "pse", "litgrid","elering", "ast", "litgrid","elering", "ast", "litgrid"],
        "time_horison": ["1D", "2D", "1D","ID", "2D", "1D","1D", "2D", "1D","ID", "2D", "1D"],
        "version": [1,2,7,8,9,6,2,3,4,9,1,5],
        "timestamp": ["2024-01-01 00:30:00", "2024-01-01 04:30:00", "2024-01-01 01:30:00","2024-01-02 10:30:00", "2024-01-01 01:30:00",
                      "2024-01-01 02:30:00","2024-03-01 09:30:00", "2024-01-02 04:30:00", "2024-01-01 01:30:00","2024-01-01 10:30:00", "2024-01-01 01:30:00", "2024-01-01 02:30:00"]
    }
    valid_models_df = pd.DataFrame(test_model_data)
    target_timestamp = "2024-01-01 00:30:00"
    target_timehorizon ="1D"
    print("Input:")
    print(valid_models_df)


    # TEST1 approach where returns dataframe with filtering possibility
    result = create_current_replacement_table2(target_timestamp,target_timehorizon, valid_models_df, conf)
    print("Output:")
    print(result)

    #SAMPLE- Filter for each TSO best timestamp
    unique_tsos_list = result["name"].unique().tolist()
    print(unique_tsos_list)
    for tso in unique_tsos_list:
        sample_tso = result.loc[(result["name"] == tso)]
        sample_tso_min = sample_tso.loc[(sample_tso["priority_hour"] == sample_tso["priority_hour"].min())
                                    & (sample_tso["priority_day"] == sample_tso["priority_day"].min())
                                    & (sample_tso["priority_business"] == sample_tso["priority_business"].min())
                                    & (sample_tso["version"] == sample_tso["version"].max())]
        print(sample_tso_min)


















