
from multiprocessing import Process, Queue, Value
import multiprocessing
import time

from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

import json
import asyncio
import ctypes
from async_request import send_process_info
from async_request import send_loss_info


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
REQUEST_ADDRESS = conf['request_address']
 

def min_scaler(_load_value):
    """Transform features by scaling each feature to a given range

    Args:
        _load_value (float64): spindle load value from cnc
    Returns:
        float: Transformed spindle load value
    """
    _load_value = _load_value * conf['max_spindle_load']
    if _load_value < conf['min_spindle_load']:
        return 0

    return _load_value


def get_row_data(_row_data_queue):
    """ put a decoded and tranformed list to multiprocess queue from consumer

    Args:
        _row_data_queue (multiprocessing.queues.Queue): put datas to queue from consumed message
    """
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
                    _op = split_decoded_msg[conf['op_code_index']]
                    _ti = split_decoded_msg[conf['time_index']]
                    _sp = min_scaler(float(split_decoded_msg[conf['load_spindle_index']]))
                    _tc = split_decoded_msg[conf['t_code_index']]
                    _ro = split_decoded_msg[conf['load_spindle_index']]
                    decoded_msg = [_op, _ti, _sp, _tc, _ro]
                    _result_list.append(decoded_msg)

                list(map(_row_data_queue.put, _result_list))

            time.sleep(SLEEP_TIME)

    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        consumer.close()


def send_row_data(_send_data_queue):
    """produce encoded message from send_data_queue even if its not empty

    Args: _send_data_queue (multiprocessing.queues.Queue): in processing datas to send through kafka producer
    """
    producer = Producer(producer_config)
    while True:
        if not _send_data_queue.empty():
            data = _send_data_queue.get()
            _time = data.split(",")[1]
            data = data.encode('utf-8')
            producer.produce(conf['topic_trend'], data)
            producer.poll(SLEEP_TIME)
        time.sleep(SLEEP_TIME)


def pre_processing(_STATE, _row_data_queue, _send_data_queue):
    """check the condition is processing state or idle state, and put datas into queue each specific situations

    Args:
        _STATE (bool): boolean for state is processing or not
        _row_data_queue (multiprocessing.queues.Queue): put datas to queue from consumed message
        _send_data_queue (multiprocessing.queues.Queue): in processing datas to send through kafka producer

    """
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
        if not _row_data_queue.empty():
            row_data = _row_data_queue.get()
            _row_data_list.append(row_data)
            cnt = 0
            zero_cnt = 0
            back_index = 1
            _send_data_csv = ''

            for i in range(len(_row_data_list)):
                scale_spindle_load = _row_data_list[i][2]
                code_data = _row_data_list[i][3]

                if code_data == conf['start_t_code'] and scale_spindle_load != 0 and _STATE != True:
                    zero_cnt = 0
                    while True:
                        prev_index = i - back_index
                        if prev_index <= 0:
                            break
                        prev_load_value = _row_data_list[prev_index][2]
                        if prev_load_value == 0:
                            print("Start")
                            _STATE = True
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
                            _send_data_queue.put(_send_data_csv)

                        _row_data_list = _row_data_list[i:]

                        if find_end == False and process_start_time != 0:
                            _STATE = False
                            process_opcode = _row_data_list[0][0]
                            process_end_time = _row_data_list[0][1]
                            process_cycle = float(process_end_time) - float(process_start_time)
                            process_count = 1
                            process_start_time = 0
                            process_count = 0
                            find_end = True
                        break

                if _STATE == True:
                    if len(_row_data_list) >= DATA_SIZE:
                        zero = 0
                        _end_index = 0
                        for j in range(len(_row_data_list)):
                            if _row_data_list[j][3] == conf['end_t_code'] and _row_data_list[j][2] == 0:
                                zero += 1
                                if(zero == 1):
                                    _end_index = j
                                if zero > conf['endpoint_zero_count']:
                                    print("Find END in PROCESSING")
                                    zero = 0
                                    _row_data_list = _row_data_list[:_end_index+2]
                                    if find_end == False and process_start_time != 0:
                                        _STATE = False
                                        process_opcode = _row_data_list[_end_index+1][0]
                                        process_end_time = _row_data_list[_end_index+1][1]
                                        process_cycle = float(process_end_time) - float(process_start_time)
                                        process_count = 1
                                        asyncio.run(send_process_info(REQUEST_ADDRESS, conf['type_cycle'], process_opcode, process_start_time, process_end_time, process_cycle, process_count))
                                        asyncio.run(send_loss_info(REQUEST_ADDRESS, conf["type_loss"], "null"))
                                        process_start_time = 0
                                        process_count = 0
                                        find_end = True
                                        _row_temp_list = _row_data_list[:]
                                        _row_data_list = []
                                    break

                        if _STATE != False:
                            data = json.dumps(_row_data_list[0:DATA_SIZE])
                            producer.produce(conf['topic_classified'], data)
                            _row_data_list = _row_data_list[1:]
                        break
        else:
            cnt += 1
            if cnt == conf['idle_count']:
                _STATE = False
                _row_data_list = []
                _send_data_csv = ''

        time.sleep(SLEEP_TIME)


def main():
    STATE = multiprocessing.Value(ctypes.c_bool, False)

    _row_data_queue = Queue()
    _send_data_queue = Queue()

    get_row_data_p = Process(target=get_row_data, args=(_row_data_queue,))
    send_row_data_p = Process(target=send_row_data, args=(_send_data_queue,))
    pre_processing_p = Process(target=pre_processing, args=(STATE.value, _row_data_queue, _send_data_queue,))

    get_row_data_p.start()
    send_row_data_p.start()
    pre_processing_p.start()

    get_row_data_p.join()
    send_row_data_p.join()
    pre_processing_p.join()


if __name__ == "__main__":
    main()
