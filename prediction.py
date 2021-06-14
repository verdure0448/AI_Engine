import asyncio
import aiohttp
from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import KafkaException

import tensorflow as tf

from sklearn.metrics import mean_absolute_error
import numpy as np
import json
import aiohttp
import asyncio

from pyspark.sql.functions import count

async def send_loss_info(_conf, _loss):
    async with aiohttp.ClientSession() as session:
        _request_info = _conf['request_address'] + _conf['type_loss'] + "?"
        _request_param = "loss=" + str(_loss)
        try:
            async with session.get(_request_info + _request_param) as response:
                result = await response.read()
        except aiohttp.ClientConnectionError as e:
            print("loss_connection error", str(e))

def decode_data(_conf, _messages):
    decode_msgs = str(_messages.value().decode('utf-8'))
    line_msgs = decode_msgs.split("\n")
    result_list = []

    for i in range(len(line_msgs)):
        split_decoded_msg = line_msgs[i].split(",")
        result_list.append(split_decoded_msg)

    return result_list


def create_signal(_np_data_array):
    _signal_data_list = _np_data_array[:, None]
    _signal_data = np.array(_signal_data_list)

    return _signal_data


def prediction(_conf, model, np_data_array,):
    prediction_list = []
    data_x_list = []
    signal_data = create_signal(np_data_array)
    learning_data = signal_data[0:_conf['predict_index'], 0]
    data_x_list.append(learning_data)
    create_data = np.array(data_x_list)
    signal_data = np.reshape(create_data, (create_data.shape[0], create_data.shape[1], 1))
    prediction_data = model.predict(signal_data, 1)
    prediction_list.append(prediction_data[0, 0])

    return prediction_list


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
    _code_with_loss = str(_data[_predict_index][0]) + "," + _data[_predict_index][1]  + "," + str(_mae)
    print(_code_with_loss)
    asyncio.run(send_loss_info(_conf, _mae))
    _send_data_list.append([_data[_predict_index][0], _data[_predict_index][1], str(_data[_predict_index][2]), _data[_predict_index][3], _data[_predict_index][4], str(result[0]), str(_mae)])

    return _send_data_list


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
        _model = tf.keras.models.load_model(_conf['model_path'])
        consumer = Consumer(consumer_config)
        consumer.subscribe([_conf['topic_consumer']])
        producer = Producer(producer_config)
        cnt = 0
        while True:
            message = consumer.poll(timeout=_conf['sleep_time'])
            if message is None:
                cnt += 1
                #if cnt == _conf['idle_count']:
                #TODO all clear
                """
                if cnt > 1000:
                    print("more then 3 secs")
                """
                continue
                
            if message.error():
                raise KafkaException(message.error())
            else:
                _result_csv = ''
                cnt = 0
                _data = json.loads(message.value().decode('utf-8'))
                _result = anomaly_detection(_conf, _model, _data)
                _result = json.dumps(_result[0])
                _result_replace = _result.replace('"','')
                _result_list = _result_replace.strip('][').split(', ')
                _result_csv = ','.join(_result_list)
                print(_result_csv)
                producer.produce(_conf['topic_predict'], _result_csv)

    except Exception:
        import traceback
        print(traceback.format_exc())

    finally:
        consumer.close()

def main():
    with open("/home/rnd01/workspace/cnc_analyzer/config_predict.json") as jsonFile:
        _conf = json.load(jsonFile)

    get_row_data(_conf)

if __name__ == "__main__":
    main()
