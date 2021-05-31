from multiprocessing import Process, Queue, process
import multiprocessing
import time

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

import tensorflow as tf

from sklearn.metrics import mean_squared_error

import numpy as np
import json
import math
import asyncio
import requests
import pandas as pd

from tensorflow.python.ops.gen_array_ops import empty

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
STATE = NONE
SLEEP_TIME = conf['SLEEP_TIME']
DATA_SIZE = conf['DATA_SIZE']
PREDICT_INDEX = conf['PREDICT_INDEX']
TOPIC_PRODUCER = conf['topic_producer']
REQUEST_ADDRESS = conf['request_address']

async def send_process_info(_opcode, _start_time, _end_time, _cycle, _count):
    _request_info = REQUEST_ADDRESS + conf['type_cycle'] + "?"
    _request_param = "opCode=" + str(_opcode) + "&startTime=" + str(_start_time) + "&endTime=" + str(
        _end_time) + "&cycleTime=" + str(_cycle) + "&count=" + str(_count)
    print(_request_info + _request_param)
    requests.get(_request_info + _request_param)
    await asyncio.sleep(SLEEP_TIME)


async def status_info(_status):
    _request_info = REQUEST_ADDRESS + conf['type_work'] + "?"
    if _status == True:
        _request_param = "work=start"
    else:
        _request_param = "work=stop"
    print(_request_info + _request_param)
    requests.get(_request_info + _request_param)
    await asyncio.sleep(SLEEP_TIME)


def create_signal(np_data_array):
    signal_data_list = np_data_array[:, None]
    signal_data = np.array(signal_data_list)

    return signal_data


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
    mse = mean_squared_error(real_data, result)
    rmse = math.sqrt(mse)
    #mse_keras = keras.losses.mean_squared_error(real_data, result)
    #mae = mean_absolute_error(real_data, result)

    return rmse


def scale_row_data(spindle_load, t_code):
    if t_code == 'T1000' or t_code == 'T1010' or t_code == 'T1100':
        t_code = conf['end_t_code']
    if spindle_load < conf['min_spindle_load']:
        spindle_load = 0
    elif spindle_load > conf['max_spindle_load']:
        spindle_load = conf['max_spindle_load']
    scale_spindle_load = spindle_load/conf['max_spindle_load']

    return scale_spindle_load, t_code


def decode_data(kafka_message):
    decode_msg = str(kafka_message.value().decode('utf-8'))
    split_decoded_msg = decode_msg.split(",")
    op_code = split_decoded_msg[conf['op_code_index']]
    time = split_decoded_msg[conf['time_index']]
    spindle_load = int(split_decoded_msg[conf['load_spindle_index']])
    t_code = split_decoded_msg[conf['t_code_index']]
    scale_spindle_load, t_code = scale_row_data(spindle_load, t_code)
    decoded_msg = [op_code, time, spindle_load, t_code, scale_spindle_load]

    return decoded_msg


def get_row_data():
    try:
        consumer = Consumer(consumer_config)
        consumer.subscribe([conf['topic_consumer']])
        while True:
            message = consumer.poll(timeout=SLEEP_TIME)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                decoded_msg = decode_data(message)
                row_data_queue.put(decoded_msg)
            time.sleep(SLEEP_TIME)

    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        consumer.close()


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def send_row_data():
    producer = Producer(producer_config)
    cnt = 0
    while True:
        if not put_row_queue.empty():
            # loop_true = asyncio.get_event_loop()
            # loop_true.run_until_complete(status_info(True))
            # loop_true.close()
            data = put_row_queue.get()
            data = data.encode('utf-8')
            producer.produce(TOPIC_PRODUCER, data)
            producer.poll(SLEEP_TIME)
        # else:
        #     loop_false = asyncio.get_event_loop()
        #     loop_false.run_until_complete(status_info(False))
        #     loop_false.close()
        if not loss_data_queue.empty():
            loss_data = "OP10-1," + str(loss_data_queue.get())
            loss_data = loss_data.encode('utf-8')
            producer.produce(TOPIC_PRODUCER, loss_data)
            producer.poll(SLEEP_TIME)
        cnt += 1
        time.sleep(SLEEP_TIME)


def pre_processing():
    global STATE
    back_index = 1
    row_data_list = []
    process_start_time = 0
    process_end_time = 0
    process_cycle = 0
    process_count = 0
    find_end = False

    while True:
        if not row_data_queue.empty():
            row_data = row_data_queue.get()
            row_data_list.append(row_data)
            zero_cnt = 0

            for i in range(len(row_data_list)):
                scale_spindle_load = row_data_list[i][4]
                code_data = row_data_list[i][3]
                if code_data == conf['end_t_code'] and scale_spindle_load == 0:
                    zero_cnt += 1
                    if zero_cnt > conf['continue_zero_count']:
                        row_data_list = row_data_list[i:]
                        zero_cnt = 0
                        print("Find END")
                        if find_end == False and process_start_time != 0:
                            process_opcode = row_data_list[0][0]
                            process_end_time = row_data_list[0][1]
                            process_cycle = float(process_end_time) - float(process_start_time)
                            process_count = 1
                            # loop = asyncio.get_event_loop()
                            # loop.run_until_complete(send_process_info(process_opcode, process_start_time, process_end_time, process_cycle, process_count))
                            # loop.close()
                            process_start_time = 0
                            process_count = 0
                            find_end = True
                        STATE = IDLE
                        break

                if STATE == IDLE:
                    if code_data == conf['start_t_code'] and scale_spindle_load != 0:
                        while True:
                            prev_index = i - back_index
                            if prev_index <= 0:
                                break
                            prev_load_value = row_data_list[prev_index][4]
                            if prev_load_value == 0:
                                print("Start")
                                STATE = PROCESSING
                                process_start_time = row_data_list[prev_index][1]
                                find_end = False
                                row_data_list = row_data_list[prev_index:]
                                zero_cnt = 0
                                back_index = 1
                                break
                            else:
                                back_index += 1

                if STATE == PROCESSING:
                    if len(row_data_list) >= DATA_SIZE:
                        pre_processed_queue.put(row_data_list[0:DATA_SIZE])
                        row_data_list = row_data_list[2:]
                        break

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


def row_data_rolling(_row_data_list):
    _row_data_df = pd.DataFrame(_row_data_list, columns=['opcode', 'time', 'load', 'tcode'])
    _row_data_rolling_df = _row_data_df.rolling(window=conf['rolling_window'], min_periods=1).mean()
    _row_data_df['load'] = _row_data_rolling_df.values
    _row_data_rolling_array = np.array(_row_data_df.values.tolist())

    return _row_data_rolling_array


def anomaly_detection():
    model = tf.keras.models.load_model(conf['model_path'])
    while True:
        _pre_processed_list = []
        if not pre_processed_queue.empty():
            _pre_processed_list = pre_processed_queue.get()
            _row_data_mean_list = row_data_mean(_pre_processed_list)
            _row_data_mean_rolling_array = row_data_rolling(_row_data_mean_list)
            _load_data_list = list(map(float, _row_data_mean_rolling_array.T[2]))
            _np_data_array = np.array(_load_data_list)
            result = prediction(model, _np_data_array)
            _signal_data = create_signal(_np_data_array)
            _rmse = loss_function(result, _signal_data)
            loss_data_queue.put(_rmse)
            print(_rmse)
            _processed_list = _row_data_mean_rolling_array.tolist()
            _processed_list[PREDICT_INDEX].append(str(result[0]))
            _processed_list[PREDICT_INDEX][2] = str(_processed_list[PREDICT_INDEX][2])
            _send_data_csv = ','.join(_processed_list[PREDICT_INDEX])
            put_row_queue.put(_send_data_csv)

        time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    pre_processed_queue = Queue()
    row_data_queue = Queue()
    loss_data_queue = Queue()
    put_row_queue = Queue()
    disposable_queue = Queue()

#    anomaly_detection_1_p = Process(target=anomaly_detection, args=())
#    anomaly_detection_2_p = Process(target=anomaly_detection, args=())
    get_row_data_p = Process(target=get_row_data, args=())
    send_row_data_p = Process(target=send_row_data, args=())
    pre_processing_p = Process(target=pre_processing, args=())

#    anomaly_detection_1_p.start()
#    anomaly_detection_2_p.start()
    get_row_data_p.start()
    send_row_data_p.start()
    pre_processing_p.start()

#    anomaly_detection_1_p.join()
#    anomaly_detection_2_p.join()
    get_row_data_p.join()
    send_row_data_p.join()
    pre_processing_p.join()

    pool = multiprocessing.Pool(processes=2)
    pool.map(anomaly_detection)
    pool.close()
    pool.join()