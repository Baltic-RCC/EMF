from xmltodict import parse
from json import dumps
import logging
import sys

logger = logging.getLogger(__name__)


def convert(input_document):
    """
    Convert XML to JSON

    :param input_document: The incoming XML document
    :type input_document: str or bytes

    :return: JSON document and Content Type
    :rtype: bytes
    """
    logger.info("OPDM Metadata XML to JSON")
    return dumps([parse(input_document)["sm:Publish"]['sm:part'][1]['opdm:OPDMObject']], indent=4).encode(), "application/json"

if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    # TODO - add correct example
    input_xml = "saxon/model_meta.xml"

    logger.info(convert(open(input_xml, "rb").read())[0].decode())
