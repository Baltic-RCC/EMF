from saxonche import PySaxonProcessor
from lxml import etree
import logging
import time
import sys
import config
from emf.common.integrations import rabbit
from emf.common.config_parser import parse_app_properties

logger = logging.getLogger(__name__)
parse_app_properties(globals(), config.paths.xslt_service.xslt)

rabbit_service = rabbit.BlockingClient()


def run_service(stylesheet, xsd):

    while True:
        method_frame, properties, body = rabbit_service.get_single_message(queue=RMQ_QUEUE)

        if not body:
            time.sleep(10)
            continue

        response = xslt30_convert(body, stylesheet)
        is_valid = validate_xml(response, xsd)

        headers = {"file-type": "xml",
                   "business-type": "QA-report",
                   "is-valid": is_valid,
                   }
        logger.info(f"Sending message to exchange: '{RMQ_EXCHANGE}'")
        rabbit_service.publish(response, RMQ_EXCHANGE, headers=headers)


def xslt30_convert(source_file, stylesheet_file, output_file=None):
    with PySaxonProcessor() as saxon:
        xslt30 = saxon.new_xslt30_processor()

        if str == type(source_file):
            document = saxon.parse_xml(xml_file_name=source_file)
        if bytes == type(source_file):
            document = saxon.parse_xml(xml_text=source_file.decode("utf-8"))

        if str == type(stylesheet_file):
            executable = xslt30.compile_stylesheet(stylesheet_file=stylesheet_file)
        if bytes == type(stylesheet_file):
            executable = xslt30.compile_stylesheet(stylesheet_text=stylesheet_file.decode("utf-8"))

        if output_file:
            logger.info(f"XML transform completed, output file -> {output_file}")
            executable.transform_to_file(xdm_node=document, output_file=output_file)

    return executable.transform_to_string(xdm_node=document).encode("utf-8")


def validate_xml(input_xml, schema_xml):

    logger.info(f"Validating XML against XSD")
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
        logger.error(f"XML file is invalid")

    return is_valid


if __name__ == '__main__':
    # Testing

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    test_file = "passed_igm_report.xml"
    test_stylesheet_file = "IGM_entsoeQAReport_Level_8.xsl"
    xsd_file = 'QAR_v2.6.1.xsd'
    test_output_file = "test_results.xml"
    test_output_file2 = "test_results2.xml"

    with open('IGM_entsoeQAReport_Level_8.xsl', 'rb') as file:
        xsl_bytes = file.read()
    with open('failed_igm_report.xml', 'rb') as file:
        xml_bytes = file.read()
    with open('QAR_v2.6.1.xsd', 'rb') as file:
        xsd_bytes = file.read()

    # test with file path and bytes object
    result = xslt30_convert(test_file, test_stylesheet_file)
    result2 = xslt30_convert(xml_bytes, xsl_bytes)
    validate_xml(test_output_file, xsd_file)
    validate_xml(result2, xsd_bytes)

    run_service(xsl_bytes, xsd_bytes)

    print('Script finished')
