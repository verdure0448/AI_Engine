from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException
from sklearn.metrics import mean_absolute_error
from multiprocessing import Event

import tensorflow as tf
import time
import numpy as np
import json
import asyncio

from ..http.async_request import send_loss_info
#from ..http.async_request import send_detection_info


event = Event()

def create_signal(_np_data_array):
    _signal_data_list = _np_data_array[:, None]
    _signal_data = np.array(_signal_data_list)

    return _signal_data


def split_sequences(sequence, n_steps):
    x = []
    for i in range(len(sequence)-n_steps):
        x.append(sequence[i:(i+n_steps)])

    return np.array(x) 

def prediction(_conf, model, np_data_array,):
    prediction_list = []
    data_x_list = []
    #signal_data = create_signal(np_data_array)
    #learning_data = signal_data[0:_conf['predict_index'], 0]
    #data_x_list.append(learning_data)
    #create_data = np.array(data_x_list)
    create_data = split_sequences(np_data_array[:, None], 50)
    signal_data = create_data.reshape((create_data.shape[0], 5, 1, 10, 1))
    prediction_data = model.predict(signal_data, 1)

    return prediction_data[0, 0]
    #prediction_list.append(prediction_data[0, 0])

    #return prediction_list


def loss_function(_conf, result, np_data_array):
    real_data = np_data_array[_conf['predict_index']:51, 0]
    #mse = mean_squared_error(real_data, result)
    #rmse = math.sqrt(mse)
    #mse_keras = keras.losses.mean_squared_error(real_data, result)
    mae = mean_absolute_error(real_data, result)

    return mae


def anomaly_detection(_conf, _model, _data):
    _send_data_list = []
    _predict_index = _conf["predict_index"]
    _load_data_array = np.array(_data)
    _np_data_array = np.array(list(map(float, _load_data_array.T[4])))
    result = prediction(_conf, _model, _np_data_array)
    _signal_data = create_signal(_np_data_array)
    _mae = loss_function(_conf, result, _signal_data)
    _code_with_loss = str(_data[_predict_index][0]) + "," + _data[_predict_index][1] + "," + str(_mae)
    #print(_code_with_loss)
    #asyncio.run(send_detection_info(_data[_predict_index][1]))
    asyncio.run(send_loss_info(_conf["request_address"], _conf["type_loss"], _mae))
    _send_data_list.append([_data[_predict_index][0], _data[_predict_index][1], str(
        _data[_predict_index][2]), _data[_predict_index][3], _data[_predict_index][4], str(result[0]), str(_mae)])

    return _send_data_list


def get_row_data(_conf, _event):
    """
    Args:
        _event (multiprocessing.Event): An event manages a flag that can be set to true with the set() method and reset to false with the clear() method.
    """

    print("prediction_cnn start")
    consumer_config = {
        'bootstrap.servers': _conf['kafka_servers'],
        'group.id': _conf['consumer_group_id'],
        'auto.offset.reset': _conf['auto_offset_reset']
    }
    producer_config = {
        'bootstrap.servers': _conf['kafka_servers'],
        #'queue.buffering.max.ms': _conf['queue_buffering_max']
    }

    try:
        _model = tf.keras.models.load_model(_conf['model_path'])
        consumer = Consumer(consumer_config)
        consumer.subscribe([_conf['topic_consumer']])
        producer = Producer(producer_config)
        _cnt = 0
        while True:
            if _event.is_set() == True:
                print("prediction_cnn process stop")
                break
            message = consumer.poll(timeout=_conf['sleep_time'])
            if message is None:
                _cnt += 1
                continue

            if message.error():
                raise KafkaException(message.error())
            else:
                _result_csv = ''
                _cnt = 0
                _data = json.loads(message.value().decode('utf-8'))
                _result = anomaly_detection(_conf, _model, _data)
                _result = json.dumps(_result[0])
                _result_replace = _result.replace('"', '')
                _result_list = _result_replace.strip('][').split(', ')
                _result_csv = ','.join(_result_list)
                #print("[prediction]" + _result_csv)
                producer.produce(_conf['topic_predict'], _result_csv)
                producer.poll(_conf['sleep_time'])
                
            time.sleep(_conf['sleep_time'])

    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        consumer.close()


def run(_event):
    with open("/home/rnd01/workspace/cnc_analyzer/config/config_predict_cnn.json") as jsonFile:
        _conf = json.load(jsonFile)

    get_row_data(_conf, _event)


if __name__ == "__main__":
    run(event)
