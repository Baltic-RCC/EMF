import uuid

NAME = f"Baltic_GL_2024"

# Distribution part, needed for filename
DIST_ID = str(uuid.uuid4())
INSTANCE_ID = str(uuid.uuid4())
GL_FULLMODEL_ID = str(uuid.uuid4())
COORD_ID = str(uuid.uuid4())
TOLERANCE = 0.1

header_list = [
    (DIST_ID, "Type", "Distribution"),
    (DIST_ID, "label", f"{NAME}.xml"),
    # (GL_FULLMODEL_ID, "Type", "FullModel"),
    (GL_FULLMODEL_ID, 'Model.messageType', 'GL'),
    (COORD_ID, "Type", "CoordinateSystem"),
    (COORD_ID, "CoordinateSystem.crsUrn", "urn:ogc:def:crs:EPSG.4326"),
]

namespace_map = {
    "cim": "http://iec.ch/TC57/2013/CIM-schema-cim16#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "dcat": "http://www.w3.org/ns/dcat#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "at": "http://publications.europa.eu/ontology/authority/",
    "dcterms": "http://purl.org/dc/terms/",
}

