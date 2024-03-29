__author__="Alison Mukoma"
__date__="20/06/2019"
__license__="MIT"


from confluent_kafka import Consumer, KafkaError
import pprint

SETTINGS = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'mygroup',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}
def data_consumer():
    c = Consumer(SETTINGS)

    c.subscribe(['testing-hl7'])

    try:
        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                pprint.pprint('Online::Received message: {0}'.format(msg.value()))
                pprint.pprint("================= Streaming Live data From kafka To sonlinux ================")

            elif msg.error().code() == KafkaError._PARTITION_EOF:
                pprint.pprint('End of partition reached {0}/{1}'
                      .format(msg.topic(), msg.partition()))
            else:
                pprint.pprint('Error occured: {0}'.format(msg.error().str()))

    except KeyboardInterrupt:
        pass

    finally:
        c.close()

data_consumer()