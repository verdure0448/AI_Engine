from time import sleep
import confluent_kafka as kafka
from confluent_kafka import Consumer, KafkaError
from multiprocessing import Process, Queue

_consumer_config = {
    'bootstrap.servers': '9.8.100.152:9092', 
    'group.id': 'cncScichart',
    'session.timeout.ms': 6000,
    'client.id': 'q-pred-1',
    'enable.auto.commit': True,
    'default.topic.config': {'auto.offset.reset': 'latest'}
}


def on_assign(c, p):
    last_offset = c.get_watermark_offsets(p[0])
    p[0].offset = last_offset[1] - 1000 # 최근 1000개의 데이터 가져오기

    # print("last_offset:", last_offset[1])
    c.assign(p)

_topic = 'MH001001001-CNC001'
def process(q):
    print('Start subscribe_process.')
    consumer = Consumer(_consumer_config)
    consumer.subscribe([_topic], on_assign=on_assign)
    # consumer.subscribe(['MH001001001-CNC001'])
    try:
        while True:
            msg = consumer.poll(timeout=0.01)
            if msg is None:
                # print('Msg is None')
                continue
            elif not msg.error():
                # print('Msg offset[{0}]'.format(msg.offset()))
                q.put(msg.value())
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print('[ERROR] Consumer End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            else:
                print('[ERROR] Consumer Error occured: {0}'.format(msg.error().str()))
    except Exception:
        pass

    finally:
        print('[DEBUG]] Consumer close')
        consumer.close()

if __name__ == "__main__":
    q = Queue()
    process(q)