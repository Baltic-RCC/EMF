from lxml import etree
import aniso8601
from json import dumps
import logging

logger = logging.getLogger(__name__)


def convert(input_document):
    """
    Convert IEC XML schedule to ndjson

    :param input_document: The incoming iec xml
    :type input_document: bytes

    :return: XML document and Content Type
    :rtype: bytes
    """
    logger.info("Converting IEC Schedule XML to JSON")
    try:
        return dumps(parse_iec_xml(input_document), indent=4).encode(), "text/json"
    except:
        logger.error(f"Could not parse {input_document}")


def get_metadata_from_xml(xml, include_namespace=True, prefix_root=False):
    """Extract all metadata present in XML root element
    Input -> xml as lxml tree
    Output -> dictionary with metadata"""

    properties_dict = {}

    # Return empty dict if input xml element is None
    if xml is None:
        return properties_dict

    # Lets get root element and its namespace
    root_element = xml.tag.split("}")

    # Handle XML-s without namespace
    if len(root_element) == 2:
        namespace, root = root_element
    else:
        root, = root_element
        namespace = ""

    if not prefix_root:
        properties_dict["root"] = root

    if include_namespace:
        properties_dict["namespace"] = namespace[1:]

    # Lets get all children of root
    for element in xml.getchildren():

        # If element has children then it is not root meta field
        if len(element.getchildren()) == 0:

            element_data = element.tag.split("}")
            if len(element_data) == 2:
                _, element_name = element_data
            else:
                element_name, = element_data

            # If not, then lets add its name and value to properties
            # First check text field and the v attribute (ENTSO-E legacy way to keep values in XML)

            if prefix_root:
                element_name = f"{root}.{element_name}"

            if element.text:
                properties_dict[element_name] = element.text
            else:
                properties_dict[element_name] = element.get("v")

    return properties_dict


def parse_iec_xml(element_tree):
    """Parses iec xml to dictionary, meta on the same row wit value and start/end time"""

    # TODO - maybe first analyse the xml, by getting all elements and try to match names, ala point_element_name = unique_element_namelist.contains("point") etc.

    # To lxml
    xml_tree = etree.fromstring(element_tree)

    # Get message header
    message_header = get_metadata_from_xml(xml_tree)

    # Get message status
    message_status = get_metadata_from_xml(xml_tree.find("{*}docStatus"), include_namespace=False, prefix_root=True)

    # Get all periods data
    periods = xml_tree.findall('.//{*}Period')

    data_list = []

    for period in periods:

        period_meta = get_metadata_from_xml(period, include_namespace=False, prefix_root=True)
        timeseries_meta = get_metadata_from_xml(period.getparent(), include_namespace=False, prefix_root=True)
        reason_meta = get_metadata_from_xml(period.find('../{*}Reason'), include_namespace=False, prefix_root=True)
        # source_meta = {"x-data-source": f"{settings.rmq_server}/{settings.rmq_vhost}/{settings.rmq_queue}"}  # TODO later

        # whole_meta = {**message_header, **message_status, **timeseries_meta, **period_meta, **reason_meta, **source_meta}
        whole_meta = {**message_header, **message_status, **timeseries_meta, **period_meta, **reason_meta}

        # DEBUG
        #for key, value in whole_meta.items():
        #    print(key, value)

        curve_type = whole_meta.get("TimeSeries.curveType", "A01")
        resolution = aniso8601.parse_duration(period.find('{*}resolution').text)
        start_time = aniso8601.parse_datetime(period.find('.//{*}start').text)
        end_time   = aniso8601.parse_datetime(period.find('.//{*}end').text)

        points = period.findall('{*}Point')

        for n, point in enumerate(points):
            position = int(point.find("{*}position").text)
            value = float(point.find("{*}quantity").text)
            timestamp_start = (start_time + resolution * (position - 1)).replace(tzinfo=None)

            if curve_type == "A03":
                # This curve type expects values to be valid until next change or until the end of period
                if n+2 <= len(points):
                    next_position = int(points[n+1].find("{*}position").text)
                    timestamp_end = (start_time + resolution * (next_position - 1)).replace(tzinfo=None)
                else:

                    timestamp_end = end_time.replace(tzinfo=None)

            else:
                # Else the value is on only valid during specified resolution
                timestamp_end = timestamp_start + resolution

            data_list.append({"value": value,
                              "position": position,
                              "utc_start": timestamp_start.isoformat(),
                              "utc_end": timestamp_end.isoformat(),
                              **whole_meta})

    return data_list
