import kafka
import numpy as np
import math

from kafka import KafkaConsumer
from kafka import KafkaProducer
import tensorflow as tf
import tensorflow.keras as keras

from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_log_error

import json
import asyncio
import queue
from json import dumps
import copy
import csv

with open("/home/rnd01/workspace/cnc_analyzer/config.json") as jsonFile:
    conf = json.load(jsonFile)

PROCESSING = "processing"
IDLE = "idle"
NONE = "none"
STATE = NONE

model = tf.keras.models.load_model(conf['model_path'])

load_dataList = []
code_dataList = []
raw_data_list = []

raw_data_queue = queue.Queue()
send_data_queue = queue.Queue()
loss_data_queue = queue.Queue()

def create_signal(np_data_array):
    signal_data_list = np_data_array[:, None]
    signal_data = np.array(signal_data_list)

    return signal_data

def prediction(np_data_array):
    prediction_list = []
    for i in range(0, 15):
        data_x_list = []
        signal_data = create_signal(np_data_array)
        learning_data = signal_data[i:100 + i, 0]
        data_x_list.append(learning_data)
        create_data = np.array(data_x_list)
        signal_data = np.reshape(create_data, (create_data.shape[0], create_data.shape[1], 1))
        prediction_data = model.predict(signal_data, 1)
        prediction_list.append(prediction_data[0, 0])
    
    return prediction_list

def root_mean_squared_error(mse):
    rmse = math.sqrt(mse)

    return rmse

def loss_function(result, np_data_array):
    real_data = np_data_array[100:115, 0]
    mse = mean_squared_error(real_data, result)
    #mse_keras = keras.losses.mean_squared_error(real_data, result)
    #rmse = math.sqrt(mse)
    #mae = mean_absolute_error(real_data, result)

    return mse

def scale_raw_Data(spindle_load, t_code):
    if t_code=='T1000' or t_code=='T1010' or t_code=='T1100':
        t_code = conf['end_t_code']

    if spindle_load < conf['min_spindle_load']:
        spindle_load = 0
    elif spindle_load > conf['max_spindle_load']:
        spindle_load = conf['max_spindle_load']

    scale_spindle_load = spindle_load/conf['max_spindle_load']

    return scale_spindle_load, t_code

def decode_data(kafka_message):
    decode_msg = str(kafka_message.value.decode('utf-8'))
    split_decoded_msg = decode_msg.split(",")
    op_code = split_decoded_msg[conf['op_code_index']]
    time = split_decoded_msg[conf['time_index']]
    spindle_load = int(split_decoded_msg[conf['load_spindle_index']])
    t_code = split_decoded_msg[conf['t_code_index']]
    scale_spindle_load, t_code = scale_raw_Data(spindle_load, t_code)
    ################### str to float

    decoded_msg = [op_code, time, scale_spindle_load, t_code]

    return decoded_msg

async def get_raw_data(consumer):
    for kafka_message in consumer:
        decoded_msg = decode_data(kafka_message)
        raw_data_queue.put(decoded_msg)
        await asyncio.sleep(0.01)

async def send_raw_data():
    while True:
        if not send_data_queue.empty():
            data = ','.join(send_data_queue.get())
            data = data.encode('utf-8')
            producer.send(conf['topic_producer'], value = data)
            producer.flush()
        if not loss_data_queue.empty():
            loss_data = "OP10-1," + str(loss_data_queue.get())
            loss_data = loss_data.encode('utf-8')
            producer.send('cnc_test3', value = loss_data)
            producer.flush()
        await asyncio.sleep(0.01)

async def anomaly_detection():
    global STATE
    global raw_data_list
    back_index = 1

    while True:
        if not raw_data_queue.empty():
            raw_data = raw_data_queue.get()
            raw_data_list.append(raw_data)
            zero_cnt = 0
            for i in range(len(raw_data_list)):
                if raw_data_list:
                    load_data = raw_data_list[i][2]
                    code_data = raw_data_list[i][3]

                    if code_data == conf['end_t_code'] and load_data == 0:
                        zero_cnt += 1

                        if zero_cnt > conf['continue_zero_count']:
                            raw_data_list = raw_data_list[i:]
                            zero_cnt = 0
                            print("Find END")
                            STATE = IDLE
                            break

                    if STATE == IDLE:
                        if code_data == conf['start_t_code'] and load_data != 0:
                            while True:
                                prev_index = i - back_index
                                if prev_index <= 0:
                                    break
                                prev_load_value = raw_data_list[prev_index][2]
                                if  prev_load_value == 0:
                                    print("Start")
                                    STATE = PROCESSING
                                    raw_data_list = raw_data_list[prev_index:]
                                    back_index = 1
                                    break
                                else:
                                    back_index += 1

                    if STATE == PROCESSING:
                        load_data_list = list(map(float, np.array(raw_data_list).T[2]))
                        np_data_array = np.array(load_data_list)
                        if(len(load_data_list)) >= conf['criteria_len']:
                            result = prediction(np_data_array)
                            signal_data = create_signal(np_data_array)
                            mse = loss_function(result, signal_data)
                            loss_data_queue.put(mse)
                            print(mse, len(raw_data_list))

                            for j in range(15):
                                index = len(raw_data_list) - 15
                                send_data_list = copy.deepcopy(raw_data_list)
                                del send_data_list[index+j][3]
                                send_data_list[index+j][2] = str(result[j] * conf['max_spindle_load'])
                                send_data_queue.put(send_data_list[index+j])
                                send_data_list.clear()
                            
                            del raw_data_list[:15]
                            load_data_list.clear()
                            break
                        
        await asyncio.sleep(0.01)

if __name__ == "__main__":    
    consumer = KafkaConsumer(
        conf['topic_consumer'],
        bootstrap_servers = [conf['kafka_servers']],
    )
    producer = KafkaProducer(
        bootstrap_servers = [conf['kafka_servers']]
    )

    asyncio.gather(
        get_raw_data(consumer),
        anomaly_detection(),
        send_raw_data()
    )
    asyncio.get_event_loop().run_forever()