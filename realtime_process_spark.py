from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from kafka import KafkaConsumer
from kafka import KafkaProducer
import tensorflow as tf
import tensorflow.keras as keras

import json
import numpy as np
import math
import copy

from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_log_error

with open("/home/rnd01/workspace/cnc_analyzer/config.json") as jsonFile:
    conf = json.load(jsonFile)

PROCESSING = "processing"
IDLE = "idle"
NONE = "none"
STATE = NONE

#model = tf.keras.models.load_model(conf['model_path'])
model = tf.keras.models.load_model("/home/rnd01/workspace/saved-model-005-019-0.00053.h5")

load_dataList = []
code_dataList = []
row_data_list = []

appName = "ex_writeStream"
kafka_servers = "9.8.100.152:9092"
topic = "cnc_test"

zero_cnt = 0

spark = SparkSession \
    .builder \
    .appName(appName) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1') \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", topic) \
    .load()

raw_string_csv = df.select(col("value").cast("string")).alias("csv").select("csv.*")
raw_df = raw_string_csv.selectExpr(
    "split(value,',')[0] as op_code",
    "split(value,',')[1] as time",
    "split(value,',')[4] as load_spindle",
    "split(value,',')[8] as t_code",
)

def scale_raw_Data(spindle_load, t_code):
    if t_code=='T1000' or t_code=='T1010' or t_code=='T1100':
        t_code = conf['end_t_code']

    if spindle_load < conf['min_spindle_load']:
        spindle_load = 0
    elif spindle_load > conf['max_spindle_load']:
        spindle_load = conf['max_spindle_load']

    scale_spindle_load = spindle_load/conf['max_spindle_load']

    return scale_spindle_load, t_code

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

def create_signal(np_data_array):
    signal_data_list = np_data_array[:, None]
    signal_data = np.array(signal_data_list)

    return signal_data

def loss_function(result, np_data_array):
    real_data = np_data_array[100:115, 0]
    mse = mean_squared_error(real_data, result)
    #mse_keras = keras.losses.mean_squared_error(real_data, result)
    #rmse = math.sqrt(mse)
    #mae = mean_absolute_error(real_data, result)

    return mse

def process_get_row(row):
    global row_data_list
    global STATE
    
    op_code = row['op_code']
    time = row['time']
    spindle_load = int(row['load_spindle'])
    t_code = row['t_code']
    scale_spindle_load, t_code = scale_raw_Data(spindle_load, t_code)

    tmp_data_list = [op_code, time, scale_spindle_load, t_code]
    row_data_list.append(tmp_data_list)

    zero_cnt = 0
    for i in range(len(row_data_list)):
        load_data = row_data_list[i][2]
        code_data = row_data_list[i][3]

        if code_data == conf['end_t_code'] and load_data == 0:
            zero_cnt += 1

            if zero_cnt > conf['continue_zero_count']:
                print("before : ", len(row_data_list))
                row_data_list = row_data_list[i:]
                print("after : ", len(row_data_list))
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
                    prev_load_value = row_data_list[prev_index][2]
                    if  prev_load_value == 0:
                        print("Start")
                        STATE = PROCESSING
                        row_data_list = row_data_list[prev_index:]
                        back_index = 1
                        break
                    else:
                        back_index += 1
        
        if STATE == PROCESSING:
            print("PROCESSING")
            load_data_list = list(map(float, np.array(row_data_list).T[2]))
            np_data_array = np.array(load_data_list)
            if(len(load_data_list)) >= conf['criteria_len']:
                result = prediction(np_data_array)
                signal_data = create_signal(np_data_array)
                mse = loss_function(result, signal_data)
                print(mse, len(row_data_list))

                for j in range(15):
                    index = len(row_data_list) - 15
                    send_data_list = copy.deepcopy(row_data_list)
                    del send_data_list[index+j][3]
                    send_data_list[index+j][2] = str(result[j] * conf['max_spindle_load'])
                    send_data_list.clear()
                            
                del row_data_list[:15]
                load_data_list.clear()
                break
        
query = raw_df \
    .writeStream \
    .outputMode("append") \
    .foreach(process_get_row) \
    .start()

query.awaitTermination()