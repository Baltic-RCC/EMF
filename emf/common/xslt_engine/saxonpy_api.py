from saxonche import PySaxonProcessor
from lxml import etree
import logging
import os
import sys

logger = logging.getLogger(__name__)


def xslt30_convert(source_file, stylesheet_file, output_file, debug=False):
    with PySaxonProcessor() as saxon:
        xslt30 = saxon.new_xslt30_processor()
        executable = xslt30.compile_stylesheet(stylesheet_file=stylesheet_file)

        if str == type(source_file):
            document = saxon.parse_xml(xml_file_name=source_file)

        if bytes == type(source_file):
            document = saxon.parse_xml(xml_text=source_file.decode("utf-8"))

        if not debug:
            output_file = os.path.abspath(f'../../../config/report_publisher/{output_file}')

        executable.transform_to_file(xdm_node=document, output_file=output_file)
        logger.info(f"XML transform completed, output file -> {output_file}")


def validate_xml(input_xml, schema_xml):
    logger.info(f"Validating {input_xml} against {schema_xml}")
    document = etree.parse(input_xml)
    schema = etree.XMLSchema(etree.parse(schema_xml))
    is_valid = schema.validate(document)
    for error in schema.error_log:
        logger.error(error)

    if is_valid:
        logger.info(f"File is valid -> {input_xml}")
    if not is_valid:
        logger.error(f"File is invalid -> {input_xml}")

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

    with open('failed_igm_report.xml', 'rb') as file:
        xml_bytes = file.read()

    # test with file path and bytes object
    xslt30_convert(test_file, test_stylesheet_file, test_output_file, debug=True)
    xslt30_convert(xml_bytes, test_stylesheet_file, test_output_file2, debug=True)

    validate_xml(test_output_file, xsd_file)
    validate_xml(test_output_file2, xsd_file)

    print('Test script finished')
