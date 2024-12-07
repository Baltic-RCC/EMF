import uuid
import config
import logging
from pathlib import Path
from emf.common.integrations import rabbit, edx
from emf.common.config_parser import parse_app_properties
from emf.common.xslt_engine.saxonpy_api import validate_xml
import xmltodict

logger = logging.getLogger(__name__)
parse_app_properties(caller_globals=globals(), path=config.paths.report_publisher.report_publisher)

rabbit_service = rabbit.BlockingClient()
edx_service = edx.EDX()


def run_service(from_queue):

    logger.info(f"Report publisher started")
    rabbit_service.consume_start(queue=from_queue, callback=send_qar)


def send_qar(channel, method, properties, body: str, schema_xml_path=Path(__file__).parent.parent.joinpath(XSD_PATH)):

    with open(schema_xml_path, 'rb') as file:
        xsd_bytes = file.read()
    is_valid = validate_xml(body, xsd_bytes)

    # Upload external QAR report
    if is_valid:
        data = xmltodict.parse(body)
        ba_message_id = BA_MESSAGE_ID
        process_id = data['Result']['ModelInformation']['MetaData'].get('conversationId')

        message_id = edx_service.send_message(receiver_EIC=MESSAGE_RECEIVER_EIC,
                                              business_type=MESSAGE_BUSINESS_TYPE,
                                              content=body,
                                              ba_message_id=ba_message_id,
                                              conversation_id=process_id)
        logger.info(f"Sent QAR via EDX with Message ID -> {message_id}")

    else:
        logger.info(f'QAR XSD validation failed, message not sent -> {schema_xml_path}')

    return channel, method, properties, body


run_service(RMQ_QUEUE)


if __name__ == "__main__":
    # Testing
    import sys

    logging.basicConfig(stream=sys.stdout,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    run_service('xslt-endpoint')

    print('')
