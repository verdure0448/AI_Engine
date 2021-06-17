from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

import time
import json

def _convert_t_code(_t_code, _end_t_code):
    """Returns 'T0000' as zero code or origin Tcode

    Args:
        _t_code (str): Tool selection code from CNC
        _end_t_code (str): end code determined by policy

    Returns:
        str: converted tool selection code
    """
    if _t_code == 'T1000' or _t_code == 'T1010' or _t_code == 'T1100':
        _t_code = _end_t_code

    return _t_code

def _decode_data(_conf, _messages):
    """Returns decoded message list, [op_code, time, spindle_load, t_code]
    messages decode using utf-8 and split by '\n' keyword
    extract and put in list some useful data from decoded messages

    Args:
        _conf (dict): configuration which loaded file as json
        _messages (cimpl.Message): consumed confluent_kafka.Message 
        
    Returns:
        list: decoded confluent_kafka.Message
    """
    _decode_msgs = str(_messages.value().decode('utf-8'))
    _line_msgs = _decode_msgs.split("\n")
    _message_list = []

    for i in range(len(_line_msgs)):
        _split_decoded_msg = _line_msgs[i].split(",")
        _op_code = _split_decoded_msg[_conf['op_code_index']]
        _time = _split_decoded_msg[_conf['time_index']]
        _spindle_load = float(_split_decoded_msg[_conf['load_spindle_index']])
        _t_code = _split_decoded_msg[_conf['t_code_index']]
        _t_code = _convert_t_code(_t_code, _conf['end_t_code'])
        _decoded_msg = [_op_code, _time, _spindle_load, _t_code]
        _message_list.append(_decoded_msg)

    return _message_list


def _row_data_mean(_row_data_list):
    '''Returns mean datas of list
    a multiple of two indexed data and a following data divied two after combined

    Args:
        _row_data_list (list): list decoded by decode_data()

    Returns:
        list: combined and divided list
    '''
    _mean_data_list = []
    
    for i in range(int(len(_row_data_list)/2)):
        _op_code = _row_data_list[i*2][0]
        _time = _row_data_list[i*2][1]
        _code = _row_data_list[i*2][3]
        _load1 = _row_data_list[i*2][2]
        _load2 = _row_data_list[i*2+1][2]
        _mean_data_list.append([_op_code, _time, (_load1 + _load2)/2, _code])

    return _mean_data_list


def _row_data_rolling(_decoded_mean_msg_list, _window_size, _max_spindle_load):
    """Computes a rolling median of a vector of floats and returns the results

    Args:
        _decoded_mean_msg_list (list): the list decoded and calculated the average
        _window_size (int): rolling window size
        _max_spindle_load (int): largest number for dividing to express datas 0 to 1

    Returns:
        list: Returns list caculated the rolling window and divided _max_spindle_load to express 0 to 1
    """
    _rolling_list = []
    _load_list = [j[2] for j in _decoded_mean_msg_list]
    for i in range(len(_load_list)-_window_size + 1):
        _load_value = (sum(_load_list[i:_window_size+i])/_window_size)/_max_spindle_load
        _op = _decoded_mean_msg_list[_window_size+i-1][0]
        _ti = _decoded_mean_msg_list[_window_size+i-1][1]
        _tc = _decoded_mean_msg_list[_window_size+i-1][3]
        _rolling_list.append([_op, _ti, _load_value,_tc])

    return _rolling_list


def _get_row_data(_conf):
    """produce encoded message which is decoded, converted, calculated the average and rolled from consumer

    Args:
        _conf (dict): configuration which loaded file as json
    """
    _consumer_config = {
        'bootstrap.servers': _conf['kafka_servers'],
        'group.id': _conf['consumer_group_id'],
        'auto.offset.reset': _conf['auto_offset_reset']
    }
    _producer_config = {
                   'bootstrap.servers': _conf['kafka_servers']
    }

    try:
        _cnt = 0
        _decoded_msg_list = []
        _decoded_mean_msg_list = []
        _consumer = Consumer(_consumer_config)
        _consumer.subscribe([_conf['topic_consumer']])
        _producer = Producer(_producer_config)
        
        while True:
            _message = _consumer.poll(timeout=_conf['sleep_time'])
            if _message is None:
                _cnt += 1
                if _cnt > _conf['idle_count']:
                    _decoded_msg_list = []
                    _decoded_mean_msg_list = []
                    print("more then 500")
                    _cnt = 0
                continue
            if _message.error():
                raise KafkaException(_message.error())
            else:
                _cnt = 0
                decoded_msgs = _decode_data(_conf, _message)
                for i in range(len(decoded_msgs)):
                    _decoded_msg_list.append(decoded_msgs[i])

                if len(_decoded_msg_list) > 1:
                    _add_decode_msgs = _row_data_mean(_decoded_msg_list)
            
                    for i in range(len(_add_decode_msgs)):
                        _decoded_mean_msg_list.append(_add_decode_msgs[i])
            
                    if len(_decoded_msg_list)%2 == 0:
                        _decoded_msg_list = []
                    else:
                        _decoded_msg_list = _decoded_msg_list[-1:]
                else:
                    continue

                if len(_decoded_mean_msg_list) < _conf["rolling_window"]:
                    continue

                _add_roll_list = _row_data_rolling(_decoded_mean_msg_list, _conf["rolling_window"], _conf["max_spindle_load"])
                _decoded_mean_msg_list = _decoded_mean_msg_list[len(_add_roll_list):]

                for i in range(len(_add_roll_list)):
                    _add_roll_data = _add_roll_list[i]
                    _message = _add_roll_data[0] + "," + str(_add_roll_data[1]) + ","  + str(_add_roll_data[2]) + ","  + _add_roll_data[3]
                    _data = _message.encode('utf-8')
                    _producer.produce(_conf['topic_producer'], _data)
                    _producer.poll(_conf['sleep_time'])
                    print(_data)

            time.sleep(_conf['sleep_time'])

    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        _consumer.close()

def run():
    #while exit_event.is_set != True:

    _conf = None
    with open("/home/rnd01/workspace/cnc_analyzer/module/config_preprocessing.json") as jsonFile:
        _conf = json.load(jsonFile)

    _get_row_data(_conf)

if __name__ == "__main__":
    run()
