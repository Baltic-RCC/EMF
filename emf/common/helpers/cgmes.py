import logging
import json
import pandas as pd
import config
from lxml import etree

logger = logging.getLogger(__name__)


def export_to_cgmes_zip(triplets: list):
    namespace_map = {
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
        "md": "http://iec.ch/TC57/61970-552/ModelDescription/1#",
        "entsoe": "http://entsoe.eu/CIM/SchemaExtension/3/1#",
    }

    rdf_map = json.load(config.paths.cgm_worker.CGMES_v2_4_15_2014_08_07)

    return pd.concat(triplets, ignore_index=True).export_to_cimxml(rdf_map=rdf_map,
                                                                   namespace_map=namespace_map,
                                                                   export_undefined=False,
                                                                   export_type="xml_per_instance_zip_per_xml",
                                                                   debug=False,
                                                                   export_to_memory=True)


def get_metadata_from_rdfxml(parsed_xml: etree._ElementTree):
    """Parse model metadata form xml, returns a dictionary with metadata"""

    assert isinstance(parsed_xml, etree._ElementTree)

    rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

    header = parsed_xml.find("{*}FullModel")  # Version update proof, as long as element name remains the same
    meta_elements = header.getchildren()

    # Add model ID from FullModel@about
    metadata = {"Model.mRID": header.attrib.get(f'{{{rdf}}}about').split(":")[-1]}

    # Add all other metadata
    for element in meta_elements:
        key = element.tag.split("}")[1]
        value = element.text if element.text else element.attrib.get(f"{{{rdf}}}resource")
        if existing_value := metadata.get(key):
            value = existing_value + [value] if isinstance(existing_value, list) else [existing_value, value]
        metadata[key] = value

    return metadata


if __name__ == "__main__":
    pass