from datetime import datetime
from multiprocessing import Process, Queue, Value
import multiprocessing
import time

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

import tensorflow as tf

from sklearn.metrics import mean_absolute_error

import numpy as np
import json
import math
import asyncio
import aiohttp
import pandas as pd
import ctypes

from tensorflow.python.ops.gen_array_ops import Empty, empty

with open("/home/rnd01/workspace/cnc_analyzer/config.json") as jsonFile:
    conf = json.load(jsonFile)

consumer_config = {
    'bootstrap.servers': conf['kafka_servers'],
    'group.id': conf['consumer_group_id'],
    'auto.offset.reset': conf['auto_offset_reset']
}

producer_config = {
    'bootstrap.servers': conf['kafka_servers']
}

PROCESSING = "processing"
IDLE = "idle"
NONE = "none"

SLEEP_TIME = conf['sleep_time']
DATA_SIZE = conf['data_size']
PREDICT_INDEX = conf['predict_index']
REQUEST_ADDRESS = conf['request_address']


async def send_process_info(_opcode, _start_time, _end_time, _cycle, _count):
    async with aiohttp.ClientSession() as session:
        _request_info = REQUEST_ADDRESS + conf['type_cycle'] + "?"
        _request_param = "opCode=" + str(_opcode) + "&startTime=" + str(_start_time) + "&endTime=" + str(
            _end_time) + "&cycleTime=" + str(_cycle) + "&count=" + str(_count)
        try:
            async with session.get(_request_info + _request_param) as response:
                if response.status == 200:
                    result = await response.read()
                else:
                    print(response.status)
        except aiohttp.ClientConnectionError as e:
            print("process_connection error", str(e))


"""
async def status_info(_status):
    async with aiohttp.ClientSession() as session:
        _request_info = REQUEST_ADDRESS + conf['type_work'] + "?"
        if _status == True:
            _request_param = "work=start"
        else:
            _request_param = "work=stop"
        try:
            async with session.get(_request_info + _request_param) as response:
                result = await response.read()
        except aiohttp.ClientConnectionError as e:
            print("status_connection error", str(e))
"""


async def send_loss_info(_loss):
    async with aiohttp.ClientSession() as session:
        _request_info = REQUEST_ADDRESS + conf['type_loss'] + "?"
        _request_param = "loss=" + str(_loss)
        try:
            async with session.get(_request_info + _request_param) as response:
                result = await response.read()
        except aiohttp.ClientConnectionError as e:
            print("loss_connection error", str(e))


async def data_empty():
    queue_clear()
    async with aiohttp.ClientSession() as session:
        _request_info = REQUEST_ADDRESS + conf['type_empty']
        try:
            async with session.get(_request_info) as response:
                result = await response.read()            
        except aiohttp.ClientConnectionError as e:
            print("loss_connection error", str(e))



def queue_clear():
    if not row_data_queue.empty():
        row_data_queue.get_nowait()
    if not pre_processed_queue.empty():
        pre_processed_queue.get_nowait()
    if not loss_data_queue.empty():
        loss_data_queue.get_nowait()
    if not put_row_queue.empty():
        put_row_queue.get_nowait()


def create_signal(_np_data_array):
    _signal_data_list = _np_data_array[:, None]
    _signal_data = np.array(_signal_data_list)

    return _signal_data


def prediction(model, np_data_array):
    prediction_list = []
    data_x_list = []
    signal_data = create_signal(np_data_array)
    learning_data = signal_data[0:PREDICT_INDEX, 0]
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
    real_data = np_data_array[PREDICT_INDEX:51, 0]
    #mse = mean_squared_error(real_data, result)
    #rmse = math.sqrt(mse)
    #mse_keras = keras.losses.mean_squared_error(real_data, result)
    mae = mean_absolute_error(real_data, result)

    return mae


def convert_end_code(t_code):
    if t_code == 'T1000' or t_code == 'T1010' or t_code == 'T1100':
        t_code = conf['end_t_code']

    return t_code


def decode_data(kafka_message):
    decode_msg = str(kafka_message.value().decode('utf-8'))
    split_decoded_msg = decode_msg.split(",")
    op_code = split_decoded_msg[conf['op_code_index']]
    time = split_decoded_msg[conf['time_index']]
    spindle_load = float(split_decoded_msg[conf['load_spindle_index']])
    t_code = split_decoded_msg[conf['t_code_index']]
    t_code = convert_end_code(t_code)
    decoded_msg = [op_code, time, spindle_load, t_code]

    return decoded_msg


def get_row_data():
    try:
        _decoded_msg_list = []
        consumer = Consumer(consumer_config)
        consumer.subscribe([conf['topic_consumer']])
        cnt = 0
        while True:
            message = consumer.poll(timeout=SLEEP_TIME)
            if message is None:
                cnt += 1
                if cnt == conf['idle_count']:
                    _decoded_msg_list = []
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                cnt = 0
                decoded_msg = decode_data(message)
                _decoded_msg_list.append(decoded_msg)
                _decoded_mean_msg_list = []
                _decoded_rolling_msg_list = []
                if len(_decoded_msg_list) == 60:
                    _decoded_mean_msg_list = row_data_mean(_decoded_msg_list)
                    _decoded_rolling_msg_list = row_data_rolling(_decoded_mean_msg_list)
                    list(map(row_data_queue.put, _decoded_rolling_msg_list))
                elif len(_decoded_msg_list) > 60 and len(_decoded_msg_list)%2 == 0:
                    _decoded_mean_msg_list = row_data_mean(_decoded_msg_list)
                    _decoded_rolling_msg_list = row_data_rolling(_decoded_mean_msg_list)
                    _decoded_msg_list = _decoded_msg_list[2:]
                    list(map(row_data_queue.put, _decoded_rolling_msg_list[30:]))

            time.sleep(SLEEP_TIME)

    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        consumer.close()


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def time_to_strtime(_time):
    _float_time = float(_time)/1e3
    dt = datetime.fromtimestamp(_float_time)

    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
        

def send_row_data():
    producer = Producer(producer_config)
    while True:
        if not put_row_queue.empty():
            data = put_row_queue.get()
            _time = data.split(",")[1]
            strtime = time_to_strtime(_time)
            data = data.encode('utf-8')
            producer.produce(conf['topic_predict'], data)
            producer.poll(SLEEP_TIME)
        if not loss_data_queue.empty():
            loss_data = loss_data_queue.get()
            loss_data = loss_data.encode('utf-8')
            producer.produce(conf['topic_loss'], loss_data)
            producer.poll(SLEEP_TIME)
        time.sleep(SLEEP_TIME)


def pre_processing(STATE):
    _row_data_list = []
    process_start_time = 0
    process_end_time = 0
    process_cycle = 0
    process_count = 0
    find_end = False
    cnt = 0

    while True:
        if not row_data_queue.empty():
            row_data = row_data_queue.get()
            _row_data_list.append(row_data)
            cnt = 0
            zero_cnt = 0
            back_index = 1
            _send_data_csv = ''
            _zero_list = []
            for i in range(len(_row_data_list)):
                scale_spindle_load = _row_data_list[i][2]
                code_data = _row_data_list[i][3]

                if code_data == conf['start_t_code'] and scale_spindle_load != 0:
                    while True:
                        prev_index = i - back_index
                        if prev_index <= 0:
                            break
                        prev_load_value = _row_data_list[prev_index][2]
                        if prev_load_value == 0:
                            print("Start")
                            STATE = True
                            #asyncio.run(status_info(STATE))
                            process_start_time = _row_data_list[prev_index][1]
                            find_end = False
                            _row_data_list = _row_data_list[prev_index:]
                            back_index = 1
                            break
                        else:
                            back_index += 1

                if code_data == conf['end_t_code'] and scale_spindle_load == 0:
                    zero_cnt += 1
                    _zero_list.append(i)
                    if zero_cnt > conf['continue_zero_count']:
                        print("Find END")
                        zero_cnt = 0
                        #while j in range(len(_row_data_list[:i])):
                        for j in range(len(_row_data_list[:i])):
                            _op = _row_data_list[j][0]
                            _ti = _row_data_list[j][1]
                            _ro = str(_row_data_list[j][2])
                            _tc = _row_data_list[j][3]
                            _sc = str(_row_data_list[j][4])
                            _pr = str(0)
                            _row_data = [_op, _ti, _ro, _tc, _sc, _pr]
                            _send_data_csv = ','.join(_row_data)

                            put_row_queue.put(_send_data_csv)
                            _loss_data = [_op, _ti, _pr]
                            _code_with_loss = ','.join(_loss_data)
                            loss_data_queue.put(_code_with_loss)

                        _row_data_list = _row_data_list[i:]

                        if find_end == False and process_start_time != 0:
                            STATE = False
                            process_opcode = _row_data_list[0][0]
                            process_end_time = _row_data_list[0][1]
                            process_cycle = float(process_end_time) - float(process_start_time)
                            process_count = 1
                            #asyncio.run(status_info(STATE))
                            process_start_time = 0
                            process_count = 0
                            find_end = True
                        break

                if STATE == True:
                    if len(_row_data_list) >= DATA_SIZE:
                        zero = 0
                        for j in range(len(_row_data_list)):
                            if _row_data_list[j][3] == conf['end_t_code'] and _row_data_list[j][2] == 0:
                                zero += 1
                                if zero > conf['continue_zero_count']:
                                    print("Find END in PROCESSING")
                                    zero = 0
                                    _row_data_list = _row_data_list[i:]
                                    if find_end == False and process_start_time != 0:
                                        STATE = False
                                        process_opcode = _row_data_list[j][0]
                                        process_end_time = _row_data_list[j][1]
                                        process_cycle = float(process_end_time) - float(process_start_time)
                                        process_count = 1
                                        _loss_data = [process_opcode, process_end_time, str(0)]
                                        _code_with_loss = ','.join(_loss_data)
                                        loss_data_queue.put(_code_with_loss)
                                        asyncio.run(send_process_info(process_opcode, process_start_time, process_end_time, process_cycle, process_count))
                                        asyncio.run(send_loss_info("null"))
                                        process_start_time = 0
                                        process_count = 0
                                        find_end = True
                                    break

                        if STATE != False:
                            pre_processed_queue.put(_row_data_list[0:DATA_SIZE])
                            _row_data_list = _row_data_list[1:]
                        break
        else:
            cnt += 1
            if cnt == conf['idle_count']:
                STATE = False
                _row_data_list = []
                _send_data_csv = ''
                _zero_list = []
                asyncio.run(data_empty())

        time.sleep(SLEEP_TIME)


def row_data_mean(_row_data_list):
    mean_data_list = []
    for i in range(len(_row_data_list)):
        _op_code = _row_data_list[i][0]
        _time = _row_data_list[i][1]
        _load = _row_data_list[i][2]
        _code = _row_data_list[i][3]

        if i % 2 == 0:
            _load_next = _row_data_list[i+1][2]
            mean_data_list.append([_op_code, _time, (_load + _load_next)/2, _code])

    return mean_data_list

def min_scaler(load_value):
    if load_value < conf['min_spindle_load']:
        return 0

    return load_value

def zero_to_one_scaler(x):
    return x / conf['max_spindle_load']


def row_data_rolling(_row_data_list):
    _row_data_df = pd.DataFrame(_row_data_list, columns=['opcode', 'time', 'load', 'tcode'])
    _row_data_df['scaler_load'] = np.nan
    _row_data_rolling_df = _row_data_df['load'].rolling(window=conf['rolling_window'], min_periods=1).mean()
    _row_data_df['load'] = _row_data_rolling_df.apply(lambda x: min_scaler(x))
    _row_data_df['scaler_load'] = _row_data_df['load'].apply(lambda x: zero_to_one_scaler(x))
    _row_data_rolling_list = _row_data_df.values.tolist()

    return _row_data_rolling_list


def anomaly_detection():
    model = tf.keras.models.load_model(conf['model_path'])
    while True:
        _pre_processed_list = []
        _load_data_array = []
        _np_data_array = []
        _code_with_loss = ''
        _send_data_csv = ''
        if not pre_processed_queue.empty():
            _pre_processed_list = pre_processed_queue.get()
            _load_data_array = np.array(_pre_processed_list)
            _np_data_array = np.array(list(map(float, _load_data_array.T[4])))
            result = prediction(model, _np_data_array)
            _signal_data = create_signal(_np_data_array)
            _mae = loss_function(result, _signal_data)
            _code_with_loss = str(_pre_processed_list[PREDICT_INDEX][0]) + "," + _pre_processed_list[PREDICT_INDEX][1]  + "," + str(_mae)
            loss_data_queue.put(_code_with_loss)
            asyncio.run(send_loss_info(_mae))
            print(_code_with_loss)
            #print(_mae)            
            _pre_processed_list[PREDICT_INDEX].append(str(result[0]))
            _pre_processed_list[PREDICT_INDEX][4] = str(_pre_processed_list[PREDICT_INDEX][4])
            _pre_processed_list[PREDICT_INDEX][2] = str(_pre_processed_list[PREDICT_INDEX][2])
            _send_data_csv = ','.join(_pre_processed_list[PREDICT_INDEX])
            put_row_queue.put(_send_data_csv)

        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    STATE = multiprocessing.Value(ctypes.c_bool, False)

    row_data_queue = Queue()
    pre_processed_queue = Queue()
    loss_data_queue = Queue()
    put_row_queue = Queue()

    anomaly_detection_1_p = Process(target=anomaly_detection, args=())
    anomaly_detection_2_p = Process(target=anomaly_detection, args=())
    get_row_data_p = Process(target=get_row_data, args=())
    send_row_data_p = Process(target=send_row_data, args=())
    pre_processing_p = Process(target=pre_processing, args=(STATE.value,))

    anomaly_detection_1_p.start()
    anomaly_detection_2_p.start()
    get_row_data_p.start()
    send_row_data_p.start()
    pre_processing_p.start()

    anomaly_detection_1_p.join()
    anomaly_detection_2_p.join()
    get_row_data_p.join()
    send_row_data_p.join()
    pre_processing_p.join()

    """
    for i in range(5):
        p = multiprocessing.Process(target=function)
        jobs.append(p)
        p.start()
    """