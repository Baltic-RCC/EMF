import logging
import sys

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(levelname)-10s %(asctime)s.%(msecs)03d %(name)-30s %(funcName)-35s %(lineno)-5d: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)]
)

def sum_on_KEY(data, KEY, precision=1):
    return round(data.query("KEY == @KEY").VALUE.astype(float).sum(), precision)
def get_load_and_generation_ssh(data):
    logger.info("Getting Load and Generation data") # TODO add wrapper with timing and logging
    return {
        "EnergyConsumer.p": sum_on_KEY(data, 'EnergyConsumer.p'),
        "EnergyConsumer.q": sum_on_KEY(data, 'EnergyConsumer.q'),
        "RotatingMachine.p": sum_on_KEY(data, 'RotatingMachine.p'),
        "RotatingMachine.q": sum_on_KEY(data, 'RotatingMachine.q'),
    }

def type_tableview_merge(data, query):
    """function assumes that the relationship between entities can be represented with a direct link (PreviousEntity.NextEntity -> NextEntity.ID)"""
    # Split the query based on "->" to identify the sequence of merges
    steps = query.split("-")

    def clean_name(name):
        return name.replace(" ", "").rstrip("<").lstrip(">")

    def parse_type_and_attribute(type_and_attr_string, default_attr="ID"):

        if "." in type_and_attr_string:
            TYPE, ATTR = type_and_attr_string.split(".")
            MERGE_ON = type_and_attr_string
        else:
            TYPE, ATTR = type_and_attr_string, default_attr
            MERGE_ON = "ID"
        return TYPE, ATTR, MERGE_ON

    # Initial data fetching based on the first step
    previous_step = steps[0]
    previous_TYPE_and_ATTR = clean_name(previous_step)
    previous_TYPE, previous_ATTR, previous_MERGE_ON = parse_type_and_attribute(previous_TYPE_and_ATTR)

    previous_entity_data = data.type_tableview(previous_TYPE).reset_index()

    # Iterate over the entities to perform merges
    for step in steps[1:]:

        TYPE_and_ATTR = clean_name(step)
        TYPE, ATTR, MERGE_ON = parse_type_and_attribute(TYPE_and_ATTR)

        entity_data = data.type_tableview(TYPE).reset_index()

        rename_mapper = {column_name: column_name.split("_")[0] for column_name in previous_entity_data.columns if f"_{TYPE}" in column_name}
        previous_entity_data = previous_entity_data.rename(columns=rename_mapper)

        if step.startswith(">"):
            from_data, to_data = previous_entity_data, entity_data
            from_name, to_name = previous_TYPE, TYPE
            from_on, to_on = previous_MERGE_ON, MERGE_ON

        if previous_step.endswith("<"):
            to_data, from_data = previous_entity_data, entity_data
            to_name, from_name = previous_TYPE, TYPE
            to_on, from_on = previous_MERGE_ON, MERGE_ON

        # From can not be on ID, if no TYPE.KEY is provided then assume convention: PreviousEntity.NextEntity -> NextEntity.ID
        if from_on == "ID":
            from_on = f"{from_name}.{to_name}"

        previous_entity_data = from_data.merge(
            to_data,
            left_on=from_on, #f"{from_name}.{to_name}",
            right_on=to_on, #"ID",
            suffixes=(f"_{from_name}", f"_{to_name}")
        )
        previous_step = step
        previous_TYPE = TYPE
        previous_ATTR = ATTR
        previous_MERGE_ON = "ID" # Reset the Merge to ID


    return previous_entity_data

def get_tieflow_data(data):
    logger.info("Getting Tieflow data")
    try:
        tieflow_data = type_tableview_merge(data, "ControlArea<-TieFlow->Terminal->ConnectivityNode")
    except:
        try:
            tieflow_data = type_tableview_merge(data, "ControlArea<-TieFlow->Terminal->TopologicalNode")
        except Exception as e:
            logger.error(f"Failed to load Tieflow data: {e}")

    # TODO find a better way to identify HVDC
    # TODO - for CGMES3/CIM17 get also the Boundary objects and use correct field to identify HVDC
    try:
        tieflow_data["BoundaryPoint.isDirectCurrent"] = tieflow_data["IdentifiedObject.description"].str.startswith("HVDC")
    except:
        try:
            tieflow_data["BoundaryPoint.isDirectCurrent"] = tieflow_data["IdentifiedObject.description_ConnectivityNode"].str.startswith("HVDC")
        except Exception as e:
            logger.error(f"Failed to load HVDC data: {e}")

    # Add Injections
    try:
        tieflow_data = tieflow_data.merge(type_tableview_merge(data, "EquivalentInjection<-Terminal.ConductingEquipment"),
                                          left_on="ID_ConnectivityNode",
                                          right_on='Terminal.ConnectivityNode',
                                          suffixes=("", "_EquivalentInjection"))
        # Add line containers
        tieflow_data = tieflow_data.merge(data.type_tableview("Line"),
                                          left_on="ConnectivityNode.ConnectivityNodeContainer",
                                          right_on="ID",
                                          suffixes=("", "_Line"))
    except:
        try:
            tieflow_data = tieflow_data.merge(
                type_tableview_merge(data, "EquivalentInjection<-Terminal.ConductingEquipment"),
                left_on="ID_TopologicalNode",
                right_on='Terminal.TopologicalNode',
                suffixes=("", "_EquivalentInjection"))

            tieflow_data = tieflow_data.merge(data.type_tableview("Line"),
                                              left_on="TopologicalNode.ConnectivityNodeContainer",
                                              right_on="ID",
                                              suffixes=("", "_Line"))
        except Exception as e:
            print(f"Unable to map injections: {e}")


    # Add SV results
    # if sv_results := data.type_tableview("SvPowerFlow") is not None:
    try:
        tieflow_data = tieflow_data.merge(data.type_tableview("SvPowerFlow"),
                                          left_on="TieFlow.Terminal",
                                          right_on="SvPowerFlow.Terminal",
                                          suffixes=("", "_SvPowerFlow"),
                                          how="left")
        tieflow_data = tieflow_data.merge(data.type_tableview('SvVoltage'),
                                          left_on="Terminal.TopologicalNode_EquivalentInjection",
                                          right_on="SvVoltage.TopologicalNode",
                                          suffixes=("", "_SvVoltage"),
                                          how="left")
    except:
        print("No SV data available")


    # Fix some names
    tieflow_data = tieflow_data.rename(columns={
        "IdentifiedObject.energyIdentCodeEic_Terminal": "IdentifiedObject.energyIdentCodeEic_ControlArea",
        "IdentifiedObject.energyIdentCodeEic": "IdentifiedObject.energyIdentCodeEic_Line"
    })

    # Add cross borders data
    def merge_sort_strings(row, col1, col2, delimiter='-'):
        return delimiter.join(sorted([row[col1], row[col2]]))

    # Apply the function to each row
    try:
        tieflow_data['cross_border'] = tieflow_data.apply(lambda row: merge_sort_strings(row, 'ConnectivityNode.fromEndIsoCode', 'ConnectivityNode.toEndIsoCode'), axis=1)
    except:
        tieflow_data['cross_border'] = tieflow_data.apply(lambda row: merge_sort_strings(row, 'TopologicalNode.fromEndIsoCode', 'TopologicalNode.toEndIsoCode'), axis=1)

    return tieflow_data

def get_system_metrics(data, tieflow_data=None, load_and_generation=None):

    if tieflow_data is None or tieflow_data.empty:
        # Use only Interchange Control Area Tieflows
        tieflow_type = "http://iec.ch/TC57/2013/CIM-schema-cim16#ControlAreaTypeKind.Interchange"
        tieflow_data = get_tieflow_data(data)
        try:
            tieflow_data = tieflow_data.query("`ControlArea.type` == @tieflow_type")
        except:
            tieflow_data = tieflow_data[tieflow_data['ControlArea.type'] == tieflow_type]


    if load_and_generation is None or load_and_generation.empty:
        load_and_generation = get_load_and_generation_ssh(data)

    data_columns = ["EquivalentInjection.p", "EquivalentInjection.q", "SvPowerFlow.p", "SvPowerFlow.q"]

    # Calculating the absolute sum and sum for tieflow data
    tieflow_abs = tieflow_data[data_columns].abs().sum().to_dict()
    tieflow_np = tieflow_data[data_columns].sum().to_dict()

    # Summing values where BoundaryPoint.isDirectCurrent is False
    try:
        tieflow_acnp = tieflow_data.query("`BoundaryPoint.isDirectCurrent` == False")[data_columns].sum().to_dict()
    except:
        tieflow_acnp = tieflow_data[tieflow_data['BoundaryPoint.isDirectCurrent'] == False][data_columns].sum().to_dict()

    # Processing HVDC tieflow data
    try:
        tieflow_hvdc = tieflow_data.query("`BoundaryPoint.isDirectCurrent` == True")[
            ['IdentifiedObject.energyIdentCodeEic_Line'] + data_columns].set_index('IdentifiedObject.energyIdentCodeEic_Line').to_dict("index")
    except:
        try:
            tieflow_hvdc = tieflow_data[tieflow_data['BoundaryPoint.isDirectCurrent'] == True][
                ['IdentifiedObject.energyIdentCodeEic_Line'] + data_columns].set_index(
                'IdentifiedObject.energyIdentCodeEic_Line').to_dict("index")
        except:
            tieflow_hvdc = None

    # Calculating total_load, generation, and net position
    load = load_and_generation["EnergyConsumer.p"].sum()
    generation = load_and_generation["RotatingMachine.p"].sum()
    net_position = tieflow_np.get("EquivalentInjection.p", 0)  # Default to 0 if key doesn't exist

    # Calculating losses and losses coefficient
    losses = (load + generation + net_position) * -1
    tieflow_abs_ei_p = tieflow_abs.get('EquivalentInjection.p', 0)  # Default to 0 if key doesn't exist
    losses_coefficient = losses / (abs(load) + abs(generation) + tieflow_abs_ei_p) if tieflow_abs_ei_p else None

    # Returning the computed metrics as a dictionary
    result = {
        'total_load': load,
        'generation': generation,
        'losses': losses,
        'losses_coefficient': losses_coefficient,
        'tieflow_abs': tieflow_abs,
        'tieflow_np': tieflow_np,
        'tieflow_acnp': tieflow_acnp,
        'tieflow_hvdc': tieflow_hvdc,
    }

    # Fixes for ELK data storage
    result = {
        outer_key: {
            key.replace('.p', '_p').replace('.q', '_q'): value
            for key, value in inner_dict.items()
        } if isinstance(inner_dict, dict) else inner_dict
        for outer_key, inner_dict in result.items()
    }

    tieflow_hvdc = [{**inner_key, 'eic': outer_key} for outer_key, inner_key in tieflow_hvdc.items()]
    tieflow_hvdc = [
        {key.replace('.p', '_p').replace('.q', '_q'): value
            for key, value in item.items()
        } if isinstance(item, dict) else item
        for item in tieflow_hvdc
    ]
    result.update({"tieflow_hvdc": tieflow_hvdc})

    return result

if __name__ == "__main__":
    import triplets
    import pandas

    data = pandas.read_RDF([r"C:\Users\kristjan.vilgo\Elering AS\Upgrade of planning tools - Elering Base Model\Models\EMS_ENHANCED\Export_2023-06-12.zip"])
    statistics = get_system_metrics(data)