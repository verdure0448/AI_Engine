from confluent_kafka import Consumer
from confluent_kafka import KafkaException

"""
list = []
for i in range(36):
    list.append(i)


def row_data_rolling(_list, _window_size):
    rolling_list = []
    for i in range(len(_list)-_window_size):
        rolling_list.append(sum(_list[i:_window_size+i])/_window_size)

    return rolling_list

#print(row_data_rolling(list, 30))

list = list[-1:]
print(list)
"""

"""
consumer_config = {
        'bootstrap.servers': '9.8.100.151:9092',
        'group.id': 'test1',
        'auto.offset.reset': 'latest'
    }
    ### auto.offset.reset earliest

try:
    consumer = Consumer(consumer_config)
    consumer.subscribe(['test'])
"""

"""
    while True:
        message = consumer.poll(timeout=0.5)
        if message is None:
            continue
        else:
            print("len(messages) : ",len(message))
            decoded_msgs = message.value().decode('utf-8')
            print(decoded_msgs)
"""
""" 
    while True:
        messages = consumer.consume(3, 0.5)
        print("len(messages) : ", len(messages))
        if messages is None:
            continue
        for message in messages:
            decode_message = message.value().decode('utf-8')
            print(decode_message)
        #decode_message = message.value().decode('utf-8')
        #print(decode_message)

    

except Exception:
        import traceback
        print(traceback.format_exc())

finally:
    consumer.close()
"""


list_1 = [1,2,3,4,5]
list_2 = list_1[:]
list_1[0] = 3

#print(list_2[:2])

list_1 = list_1 + list_2
print(list_1)
