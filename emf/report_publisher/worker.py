import uuid
import config
import logging
from pathlib import Path
from emf.common.integrations import rabbit, edx
from emf.common.config_parser import parse_app_properties
from emf.common.xslt_engine.saxonpy_api import validate_xml

logger = logging.getLogger(__name__)
parse_app_properties(globals(), config.paths.xslt_service.xslt)

rabbit_service = rabbit.BlockingClient()
edx_service = edx.EDX()

schema_xml_path = Path(__file__).parent.joinpath('QAR_v2.6.1.xsd')


def run_service(from_queue):

    logger.info(f"Report publisher started")
    rabbit_service.consume_start(queue=from_queue, callback=send_qar)


def send_qar(channel, method, properties, body: str):

    is_valid = properties.headers.get('is-valid')

    if is_valid != 'True':
        with open(schema_xml_path, 'rb') as file:
            xsd_bytes = file.read()
        is_valid = validate_xml(body, xsd_bytes)

    # Upload external QAR report
    if is_valid:
        ba_message_id = ""
        process_id = str(uuid.uuid4())

        message_id = edx_service.send_message('SERVICE-QAS',
                                               'ENTSOE-OPDM-ValidateResult',
                                               content=body,
                                               ba_message_id=ba_message_id,
                                               conversation_id=process_id)
        logger.info(f"Sent QAR via EDX with Message ID -> {message_id}")

    else:
        logger.info(f'QAR XSD validation failed, message not sent -> {schema_xml_path}')

    return channel, method, properties, body


rabbit_queue = 'xslt-endpoint'
run_service(rabbit_queue)


if __name__ == "__main__":
    # Testing
    import sys

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    run_service('xslt-endpoint')

    print('')
