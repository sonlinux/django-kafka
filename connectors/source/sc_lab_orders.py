from confluent_kafka import Producer
import pprint

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))

p = Producer({'bootstrap.servers': '127.0.0.1:9092'})

try:
    for val in xrange(1, 1000):
        p.produce('from-python-producer-sonlinux', 'Just testing some numbers: #{0}'
                  .format(val), callback=acked)
        print
        pprint.pprint('================= Streaming data to kafka From sonlinux ================')
        print
        p.poll(0.5)

except KeyboardInterrupt:
    pass

p.flush(30)