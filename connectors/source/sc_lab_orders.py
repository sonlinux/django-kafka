__author__="Alison Mukoma"
__date__="20/06/2019"
__license__="MIT"

from confluent_kafka import Producer
import pprint

def send_to_kafka(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

try:
    data = """
    MSH|^~\&|XXXXXX||HealthOrg01||||ORU^R01|Q1111111111111111111|P|2.3|<cr>
    PID|||000000001||SMITH^JOHN||19700101|M||||||||||999999999999|123456789|<cr>
    PD1||||1234567890^LAST^FIRST^M^^^^^NPI|<cr>
    OBR|1|341856649^HNAM_ORDERID|000000000000000000|648088^Basic Metabolic 
    Panel|||20150101000100|||||||||1620^Johnson^John^R||||||20150101000100|||M|||||||||||20150101000100|<cr>
    OBX|1|NM|GLU^Glucose Lvl|159|mg/dL|65-99^65^99|H|||F|||20150101000100|
    """
    p.produce('testing-hl7', data, callback=send_to_kafka)
    pprint.pprint('================= Streaming data to kafka From sonlinux ================')

    p.poll(0.5)

    # for val in xrange(1, 5):
    #     p.produce('from-python-producer-sonlinux', 'testing HL7: #{0}'
    #               .format(val), callback=send_to_kafka)
    #     pprint.pprint('================= Streaming data to kafka From sonlinux ================')

    #     p.poll(0.5)

except KeyboardInterrupt:
    pass

p.flush(30)



