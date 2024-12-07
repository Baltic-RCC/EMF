from saxonche import PySaxonProcessor
from lxml import etree
import logging
import time
import json
import sys
import config
import os
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)
parse_app_properties(globals(), config.paths.xslt_service.xslt)

rabbit_service = rabbit.BlockingClient()


def run_service():

    logger.info(f"Shoveling from queue '{RMQ_QUEUE}' to exchange '{RMQ_EXCHANGE}'")
    rabbit_service.shovel(RMQ_QUEUE, RMQ_EXCHANGE, do_conversion)


def do_conversion(channel, method, properties, body: str):

    message_dict = json.loads(body)
    body = xslt30_convert(message_dict.get('XML'), message_dict.get('XSL'))

    if 'XSD' in message_dict.keys():
        is_valid = validate_xml(body, message_dict.get('XSD').encode("utf-8"))
    else:
        logger.warning("XSD file not found in message, report was not validated")
        is_valid = None

    properties.headers = {"file-type": "XML",
                          "business-type": "QA-report",
                          "is-valid": f"{is_valid}",
                          }

    return channel, method, properties, body


def xslt30_convert(source_file, stylesheet_file, output_file=None):
    with PySaxonProcessor() as saxon:
        xslt30 = saxon.new_xslt30_processor()

        if str == type(source_file):
            if os.path.isfile(source_file):
                document = saxon.parse_xml(xml_file_name=source_file)
            else:
                document = saxon.parse_xml(xml_text=source_file)
        if bytes == type(source_file):
            document = saxon.parse_xml(xml_text=source_file.decode("utf-8"))

        if str == type(stylesheet_file):
            if os.path.isfile(source_file):
                executable = xslt30.compile_stylesheet(stylesheet_file=stylesheet_file)
            else:
                executable = xslt30.compile_stylesheet(stylesheet_text=stylesheet_file)
        if bytes == type(stylesheet_file):
            executable = xslt30.compile_stylesheet(stylesheet_text=stylesheet_file.decode("utf-8"))

        if output_file:
            logger.info(f"XML transform completed, output file -> {output_file}")
            executable.transform_to_file(xdm_node=document, output_file=output_file)

    return executable.transform_to_string(xdm_node=document).encode("utf-8")


def validate_xml(input_xml, schema_xml):

    if str == type(input_xml):
        document = etree.parse(input_xml)
    if bytes == type(input_xml):
        document = etree.fromstring(input_xml)

    if str == type(schema_xml):
        schema = etree.XMLSchema(etree.parse(schema_xml))
    if bytes == type(schema_xml):
        schema = etree.XMLSchema(etree.fromstring(schema_xml))

    is_valid = schema.validate(document)
    for error in schema.error_log:
        logger.error(error)
    if is_valid:
        logger.info(f"XML file is valid")
    if not is_valid:
        logger.error(f"XML file is not valid")

    return is_valid


if __name__ == '__main__':
    # Testing
    from pathlib import Path

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    with open('failed_igm_report.xml', 'rb') as file:
        xml_bytes = file.read()
    with open('IGM_entsoeQAReport_Level_8.xsl', 'rb') as file:
        xsl_bytes = file.read()
    with open(Path(__file__).parent.parent.joinpath('schemas/QAR_v2.6.1.xsd'), 'rb') as file:
        xsd_bytes = file.read()

    data = {"XML": xml_bytes.decode(), "XSL": xsl_bytes.decode(), "XSD": xsd_bytes.decode()}
    message_json = json.dumps(data)

    rabbit_service.publish(message_json, 'emfos.xslt')
    logger.info(f"Sending to exchange 'emfos.xslt'")
    time.sleep(2)

    run_service()


    logger.info('Script finished')

