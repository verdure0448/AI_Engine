from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException
from multiprocessing import Event

import time
import json
import asyncio
import queue

from ..http.async_request import send_process_info
from ..http.async_request import send_loss_info


event = Event()
STATE = False 
_row_data_list = []
process_start_time = 0

def _min_scaler(_load_value, _max_spindle_value, _min_spindle_value):
    """Transform features by scaling each feature to a given range

    Args:
        _load_value (float64): spindle load value from cnc
        _max_spindle_value (int) : largest number for dividing to express datas 0 to 1
        _min_spindle_value (int) : make the 0 less than specific number

    Returns:
        float: Transformed spindle load value
    """
    _load_value = _load_value * _max_spindle_value
    if _load_value < _min_spindle_value:
        return 0

    return _load_value


def _get_row_data(_conf, _event):
    """ put a decoded and tranformed list to multiprocess queue from consumer

    Args:
        _conf (dict): configuration which loaded file as json
        _event (multiprocessing.Event): An event manages a flag that can be set to true with the set() method and reset to false with the clear() method.
    """
    _consumer_config = {
        'bootstrap.servers': _conf['kafka_servers'],
        'group.id': _conf['consumer_group_id'],
        'auto.offset.reset': _conf['auto_offset_reset']
    }

    _producer_config = {
        'bootstrap.servers': _conf['kafka_servers']
    }

    _row_data_queue = queue.Queue()
    try:
        _cnt = 0
        _consumer = Consumer(_consumer_config)
        _consumer.subscribe([_conf['topic_consumer']])
        _producer = Producer(_producer_config)

        while True:
            if _event.is_set():
                break
            _message = _consumer.poll(timeout=_conf['sleep_time'])
            if _message is None:
                _cnt += 1
                if _cnt > _conf['idle_count']:
                    print("more then 500")
                    _cnt = 0
                continue
            if _message.error():
                raise KafkaException(_message.error())
            else:
                _cnt = 0
                decode_msgs = str(_message.value().decode('utf-8'))
                line_msgs = decode_msgs.split("\n")

                for i in range(len(line_msgs)):
                    _split_decoded_msg = line_msgs[i].split(",")
                    _op = _split_decoded_msg[_conf['op_code_index']]
                    _ti = _split_decoded_msg[_conf['time_index']]
                    _sp = _min_scaler(float(_split_decoded_msg[_conf['load_spindle_index']]), _conf['max_spindle_load'], _conf['min_spindle_load'])
                    _tc = _split_decoded_msg[_conf['t_code_index']]
                    _ro = _split_decoded_msg[_conf['load_spindle_index']]
                    _decoded_msg = [_op, _ti, _sp, _tc, _ro]
                    _row_data_queue.put(_decoded_msg)

                _pre_processing(_conf, _row_data_queue, _producer)

            time.sleep(_conf['sleep_time'])

    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        _consumer.close()


def _send_row_data(_send_data_list, _producer, _topic, _polling):
    """produce encoded message from send_data_queue even if its not empty
    
    Args:
        _send_data_list (list): in processing datas to send through kafka producer
        _producer (confluent_kafka.Producer): Create a new Producer instance using the provided configuration dict
        _topic (str): topic to produce message to
        _polling (int): maximum time to block waiting for message, event or callback
    """
    _data = _send_data_list.encode('utf-8')
    _producer.produce(_topic, _data)
    _producer.poll(_polling)


def _pre_processing(_conf, _row_data_queue, _producer):
    """check the condition is processing state or idle state, and put datas into queue each specific situations

    Args:
        _conf (dict): configuration which loaded file as json
        _row_data_queue (queues.Queue): put datas to queue from consumed message
        _producer (confluent_kafka.Producer): Create a new Producer instance using the provided configuration dict
    """
    global STATE
    global _row_data_list
    global process_start_time
    _row_temp_list = []
    _process_end_time = 0
    _process_cycle = 0
    _process_count = 0
    _find_end = False
    _zero_cnt = 0
    _back_index = 1
    _send_data_csv = ''

    _row_data = _row_data_queue.get()
    _row_data_list.append(_row_data)

    for i in range(len(_row_data_list)):
        _scale_spindle_load = _row_data_list[i][2]
        _code_data = _row_data_list[i][3]

        if _code_data == _conf['start_t_code'] and _scale_spindle_load != 0 and STATE != True:
            _zero_cnt = 0
            while True:
                _prev_index = i - _back_index
                if _prev_index <= 0:
                    break
                _prev_load_value = _row_data_list[_prev_index][2]
                if _prev_load_value == 0:
                    print("Start")
                    STATE = True
                    process_start_time = _row_data_list[_prev_index+1][1]
                    _find_end = False
                    _row_data_list = _row_data_list[_prev_index+1:]
                    if len(_row_temp_list) != 0:
                        _row_data_list = _row_temp_list + _row_data_list
                        _row_temp_list = []
                    _back_index = 1
                    break
                else:
                    _back_index += 1

        if _code_data == _conf['end_t_code'] and _scale_spindle_load == 0:
            _zero_cnt += 1
            if _zero_cnt > _conf['continue_zero_count']:
                print("Find END")
                _zero_cnt = 0

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
                    _send_row_data(_send_data_csv, _producer, _conf['topic_trend'], _conf['sleep_time'])

                _row_data_list = _row_data_list[i:]

        if STATE == True:
            if len(_row_data_list) >= _conf['data_size']:
                _zero = 0
                _end_index = 0
                for j in range(len(_row_data_list)):
                    if _row_data_list[j][3] == _conf['end_t_code'] and _row_data_list[j][2] == 0:
                        _zero += 1
                        if(_zero == 1):
                            _end_index = j
                        if _zero > _conf['endpoint_zero_count']:
                            _zero = 0
                            _row_data_list = _row_data_list[:_end_index+2]
                            if _find_end == False and process_start_time != 0:
                                STATE = False
                                process_opcode = _row_data_list[_end_index+1][0]
                                _process_end_time = _row_data_list[_end_index+1][1]
                                _process_cycle = float(_process_end_time) - float(process_start_time)
                                _process_count = 1
                                asyncio.run(send_process_info(_conf['request_address'], _conf['type_cycle'], process_opcode, process_start_time, _process_end_time, _process_cycle, _process_count))
                                asyncio.run(send_loss_info(_conf['request_address'], _conf["type_loss"], "null"))
                                process_start_time = 0
                                _process_count = 0
                                _find_end = True
                                _row_temp_list = _row_data_list[:]
                                _row_data_list = []
                            break

                if STATE != False:
                    data = json.dumps(_row_data_list[0:_conf['data_size']])
                    _producer.produce(_conf['topic_classified'], data)
                    _row_data_list = _row_data_list[1:]
                break
        

def run(_event):
    _conf = None
    with open("/home/rnd01/workspace/cnc_analyzer/config/config_trend.json") as jsonFile:
        _conf = json.load(jsonFile)
    
    _get_row_data(_conf, _event)

if __name__ == "__main__":
    run(event)
