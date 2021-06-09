from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

import json

def convert_t_code(_conf, _t_code):
    if _t_code == 'T1000' or _t_code == 'T1010' or _t_code == 'T1100':
        _t_code = _conf['end_t_code']

    return _t_code

def decode_data(_conf, _messages):
    decode_msgs = str(_messages.value().decode('utf-8'))
    line_msgs = decode_msgs.split("\n")
    result_list = []

    for i in range(len(line_msgs)):
        split_decoded_msg = line_msgs[i].split(",")
        op_code = split_decoded_msg[_conf['op_code_index']]
        time = split_decoded_msg[_conf['time_index']]
        #print(split_decoded_msg[_conf['load_spindle_index']])
        spindle_load = float(split_decoded_msg[_conf['load_spindle_index']])
        t_code = split_decoded_msg[_conf['t_code_index']]
        t_code = convert_t_code(_conf, t_code)
        decoded_msg = [op_code, time, spindle_load, t_code]
        result_list.append(decoded_msg)

    return result_list


def row_data_mean(_row_data_list):
    '''
    arg:
    process:

    return:
    '''
    mean_data_list = []
    
    for i in range(int(len(_row_data_list)/2)):
        _op_code = _row_data_list[i*2][0]
        _time = _row_data_list[i*2][1]
        _code = _row_data_list[i*2][3]
        _load1 = _row_data_list[i*2][2]
        _load2 = _row_data_list[i*2+1][2]
        mean_data_list.append([_op_code, _time, (_load1 + _load2)/2, _code])

    return mean_data_list

def min_scaler(load_value):
    if load_value < 250:
        return 0

    return load_value

#row_data_rolling([j[2] for j in _decoded_mean_msg_list], 30)
#TODO parameter max add
#def row_data_rolling(_list, _window_size):
def row_data_rolling(_list, _window_size):
    rolling_list = []
    _load_list = [j[2] for j in _list]
    for i in range(len(_load_list)-_window_size + 1):
        #rolling_list.append((sum(_list[i:_window_size+i])/_window_size)/14000)
        _load_value = (sum(_load_list[i:_window_size+i])/_window_size)/14000
        _op = _list[_window_size+i-1][0]
        _ti = _list[_window_size+i-1][1]
        #_sp = min_scaler(_list[_window_size+i-1][2])
        _tc = _list[_window_size+i-1][3]
        #rolling_list.append([_op, _ti, _load_value, _tc])
        rolling_list.append([_op, _ti, _load_value,_tc])

    return rolling_list


def get_row_data(_conf):
    consumer_config = {
        'bootstrap.servers': _conf['kafka_servers'],
        'group.id': _conf['consumer_group_id'],
        'auto.offset.reset': _conf['auto_offset_reset']
    }
    producer_config = {
                   'bootstrap.servers': _conf['kafka_servers']
    }

    try:
        _decoded_msg_list = []
        consumer = Consumer(consumer_config)
        consumer.subscribe([_conf['topic_consumer']])
        producer = Producer(producer_config)
        cnt = 0
        _decoded_mean_msg_list = []
        while True:
            #message = consumer.poll(timeout=_conf['sleep_time'])
            message = consumer.poll(timeout=1)
            if message is None:
                cnt += 1
                #if cnt == _conf['idle_count']:
                #TODO all clear
                if cnt > 3:
                    _decoded_msg_list = []
                    print("more then 3 secs")
                continue
            if message.error():
                raise KafkaException(message.error())
            else:
                cnt = 0
                #TODO: when polling, polling messages are 
                
                decoded_msgs = decode_data(_conf, message)
                #print('message\n', decoded_msgs)
                # TODO _decoded_msg_list = _decoded_msg_list.extend(decoded_msgs)
                for i in range(len(decoded_msgs)):
                    _decoded_msg_list.append(decoded_msgs[i])

                if len(_decoded_msg_list) > 1:
                    add_decode_msgs = row_data_mean(_decoded_msg_list)
            
                    for i in range(len(add_decode_msgs)):
                        _decoded_mean_msg_list.append(add_decode_msgs[i])
            
                    if len(_decoded_msg_list)%2 == 0:
                        _decoded_msg_list = []
                    else:
                        _decoded_msg_list = _decoded_msg_list[-1:]
                else:
                    continue

                if len(_decoded_mean_msg_list) < 30:
                    continue

                #_add_roll_list = row_data_rolling([j[2] for j in _decoded_mean_msg_list], 30)
                _add_roll_list = row_data_rolling(_decoded_mean_msg_list, 30)
                #_decoded_rolling_msg_list.append(_add_roll_list)
                _decoded_mean_msg_list = _decoded_mean_msg_list[len(_add_roll_list):]
                #print("[DEBUG] ", len(_add_roll_list), " ", _add_roll_list)

                for i in range(len(_add_roll_list)):
                    _add_roll_data = _add_roll_list[i]
                    #[opcode, time, load_spindel, t_code]
                    messsage = _add_roll_data[0] + "," + str(_add_roll_data[1]) + ","  + str(_add_roll_data[2]) + ","  + _add_roll_data[3]
                    data = messsage.encode('utf-8')
                    producer.produce(_conf['topic_producer'], data)
                    producer.poll(1)
                    print(data)


    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        consumer.close()

def main():
    conf = None
    with open("/home/rnd01/workspace/cnc_analyzer/config_preprocessing.json") as jsonFile:
        conf = json.load(jsonFile)
        print(conf['topic_consumer'])

    get_row_data(conf)

if __name__ == "__main__":
    main()
