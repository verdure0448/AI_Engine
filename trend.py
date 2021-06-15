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

with open("/home/rnd01/workspace/cnc_analyzer/config_trend.json") as jsonFile:
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


async def send_loss_info(_loss):
    async with aiohttp.ClientSession() as session:
        _request_info = conf['request_address'] + conf['type_loss'] + "?"
        _request_param = "loss=" + str(_loss)
        try:
            async with session.get(_request_info + _request_param) as response:
                result = await response.read()
        except aiohttp.ClientConnectionError as e:
            print("loss_connection error", str(e))


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

def min_scaler(load_value):
    load_value = load_value * conf['max_spindle_load']
    if load_value < conf['min_spindle_load']:
        return 0

    return load_value


def get_row_data():
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([conf['topic_consumer']])
        cnt = 0
        while True:
            message = consumer.poll(timeout=SLEEP_TIME)
            if message is None:
                cnt += 1
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                cnt = 0
                decode_msgs = str(message.value().decode('utf-8'))
                line_msgs = decode_msgs.split("\n")
                _result_list = []

                for i in range(len(line_msgs)):
                    split_decoded_msg = line_msgs[i].split(",")
                    _op = split_decoded_msg[0]
                    _ti = split_decoded_msg[1]
                    _sp = min_scaler(float(split_decoded_msg[2]))
                    _tc = split_decoded_msg[3]
                    _ro = split_decoded_msg[2]
                    decoded_msg = [_op, _ti, _sp, _tc, _ro]
                    _result_list.append(decoded_msg)

                list(map(row_data_queue.put, _result_list))
                
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
            # print(data)
            _time = data.split(",")[1]
            strtime = time_to_strtime(_time)
            data = data.encode('utf-8')
            producer.produce(conf['topic_trend'], data)
            producer.poll(SLEEP_TIME)
        """
        if not loss_data_queue.empty():
            loss_data = loss_data_queue.get()
            loss_data = loss_data.encode('utf-8')
            producer.produce(conf['topic_loss'], loss_data)
            producer.poll(SLEEP_TIME)
        """
        time.sleep(SLEEP_TIME)


def pre_processing(STATE):
    _row_data_list = []
    _row_temp_list = []
    process_start_time = 0
    process_end_time = 0
    process_cycle = 0
    process_count = 0
    find_end = False
    cnt = 0
    producer = Producer(producer_config)

    while True:
        if not row_data_queue.empty():
            row_data = row_data_queue.get()
            _row_data_list.append(row_data)
            cnt = 0
            
            back_index = 1
            _send_data_csv = ''
    
            zero_cnt = 0
            
            for i in range(len(_row_data_list)):
                scale_spindle_load = _row_data_list[i][2]
                code_data = _row_data_list[i][3]

                if code_data == conf['start_t_code'] and scale_spindle_load != 0 and STATE != True:
                    zero_cnt = 0
                    while True:
                        prev_index = i - back_index
                        if prev_index <= 0:
                            break
                        prev_load_value = _row_data_list[prev_index][2]
                        if prev_load_value == 0:
                            print("Start")
                            STATE = True
                            #asyncio.run(status_info(STATE))
                            process_start_time = _row_data_list[prev_index+1][1]
                            find_end = False
                            _row_data_list = _row_data_list[prev_index+1:]
                            if len(_row_temp_list) != 0:
                                _row_data_list = _row_temp_list + _row_data_list
                                _row_temp_list = []
                            back_index = 1
                            break
                        else:
                            back_index += 1

                if code_data == conf['end_t_code'] and scale_spindle_load == 0:
                    zero_cnt += 1
                    if zero_cnt > conf['continue_zero_count']:
                        print("Find END")
                        zero_cnt = 0
                        
                        for j in range(len(_row_data_list[:i])):
                            _op = _row_data_list[j][0]
                            _ti = _row_data_list[j][1]
                            _ro = str(_row_data_list[j][2])
                            _tc = _row_data_list[j][3]
                            _sc = str(_row_data_list[j][4])
                            _pr = str(0)
                            _lo = str(0)
                            _row_data = [_op, _ti, _ro, _tc, _sc, _pr, _lo]
                            _send_data_csv = ','.join(_row_data)
                            put_row_queue.put(_send_data_csv)
                        
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
                        _end_index = 0
                        for j in range(len(_row_data_list)):
                            if _row_data_list[j][3] == conf['end_t_code'] and _row_data_list[j][2] == 0:
                                zero += 1
                                if(zero == 1):
                                    _end_index = j
                                if zero > 20:
                                    print("Find END in PROCESSING")
                                    zero = 0
                                    _row_data_list = _row_data_list[:_end_index+2]
                                    if find_end == False and process_start_time != 0:
                                        STATE = False
                                        process_opcode = _row_data_list[_end_index+1][0]
                                        process_end_time = _row_data_list[_end_index+1][1]
                                        process_cycle = float(process_end_time) - float(process_start_time)
                                        process_count = 1
                                        asyncio.run(send_process_info(process_opcode, process_start_time, process_end_time, process_cycle, process_count))
                                        asyncio.run(send_loss_info("null"))
                                        process_start_time = 0
                                        process_count = 0
                                        find_end = True
                                        _row_temp_list = _row_data_list[:]
                                        _row_data_list = []
                                    break
                        
                        if STATE != False:
                            #pre_processed_queue.put(_row_data_list[0:DATA_SIZE])
                            data = json.dumps(_row_data_list[0:DATA_SIZE])
                            producer.produce(conf['topic_classified'], data)
                            _row_data_list = _row_data_list[1:]
                        break
        else:
            cnt += 1
            if cnt == conf['idle_count']:
                STATE = False
                _row_data_list = []
                _send_data_csv = ''

        time.sleep(SLEEP_TIME)


def zero_to_one_scaler(x):
    return x / conf['max_spindle_load']


if __name__ == "__main__":
    STATE = multiprocessing.Value(ctypes.c_bool, False)

    row_data_queue = Queue()
    pre_processed_queue = Queue()
    #loss_data_queue = Queue()
    put_row_queue = Queue()

    get_row_data_p = Process(target=get_row_data, args=())
    send_row_data_p = Process(target=send_row_data, args=())
    pre_processing_p = Process(target=pre_processing, args=(STATE.value,))

    get_row_data_p.start()
    send_row_data_p.start()
    pre_processing_p.start()

    get_row_data_p.join()
    send_row_data_p.join()
    pre_processing_p.join()