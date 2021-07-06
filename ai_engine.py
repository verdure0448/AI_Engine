import module.process.preprocessing as preprocessing
import module.process.trend as trend
import module.process.prediction as prediction_bi
import module.process.prediction_cnn as prediction_conv2d

from multiprocessing import Process, Event

from flask import Flask
from flask_restplus import Api, Resource


sig_preprocess = Event()
sig_trend = Event()
sig_prediction = Event()

sig_prediction_1 = Event()
sig_prediction_2 = Event()

preprocess_state = False
trend_state = False
prediction_state = False

prediction_1_state = False
prediction_2_state = False

preprocessing_process = None
trend_process = None
prediction_process = None

prediction_1_process = None
prediction_2_process = None


def bi_lstm_management(_status):
    """
    """
    global sig_preprocess, sig_trend, sig_prediction
    global preprocess_state, trend_state, prediction_state

    if _status == True and preprocess_state == False and trend_state == False and prediction_state == False:
        preprocess_management(_status)
        trend_management(_status)
        prediction_management(_status)

        return True
    elif _status == False and preprocess_state == True and trend_state == True and prediction_state == True:
        preprocess_management(_status)
        trend_management(_status)
        prediction_management(_status)

        return True

    elif _status == "info" and preprocess_state == True and trend_state == True and prediction_state == True:
        _preprocess_info = preprocess_management(_status)
        _trend_info = trend_management(_status)
        _prediction_info = prediction_management(_status)

        _result = []
        _result.append(_preprocess_info)
        _result.append(_trend_info)
        _result.append(_prediction_info)

        return _result

    else:
        return False


def conv2d_lstm_management(_status):
    """
    """
    global sig_preprocess, sig_trend, sig_prediction
    global preprocess_state, trend_state, prediction_state

    if _status == True and preprocess_state == False and trend_state == False and prediction_state == False:
        preprocess_management(_status)
        trend_management(_status)
        prediction_management(_status)

        return True
    elif _status == False and preprocess_state == True and trend_state == True and prediction_state == True:
        preprocess_management(_status)
        trend_management(_status)
        prediction_management(_status)

        return True

    elif _status == "info" and preprocess_state == True and trend_state == True and prediction_state == True:
        _preprocess_info = preprocess_management(_status)
        _trend_info = trend_management(_status)
        _prediction_info = prediction_management(_status)

        _result = []
        _result.append(_preprocess_info)
        _result.append(_trend_info)
        _result.append(_prediction_info)

        return _result

    else:
        return False


def process_management(_status):
    """To manage entire process
    Entire processes start if _status True and stop all if _status false

    Args:
        _status (bool): True is start, False is stop

    Returns:
        bool: True is start or stop, False is nothing happen
    """
    global sig_preprocess, sig_trend, sig_prediction
    global preprocess_state, trend_state, prediction_state

    if _status == True and preprocess_state == False and trend_state == False and prediction_state == False:
        preprocess_management(_status)
        trend_management(_status)
        prediction_management(_status)

        return True
    elif _status == False and preprocess_state == True and trend_state == True and prediction_state == True:
        preprocess_management(_status)
        trend_management(_status)
        prediction_management(_status)

        return True

    elif _status == "info" and preprocess_state == True and trend_state == True and prediction_state == True:
        _preprocess_info = preprocess_management(_status)
        _trend_info = trend_management(_status)
        _prediction_info = prediction_management(_status)

        _result = []
        _result.append(_preprocess_info)
        _result.append(_trend_info)
        _result.append(_prediction_info)

        return _result

    else:
        return False

def preprocess_management(_status):
    """To manage preprocess
    Preprocesses start if _status True and stop all if _status false

    Args:
        _status (bool): True is start, False is stop

    Returns:
        bool: True is start or stop, False is nothing happen
    """
    global sig_preprocess
    global preprocess_state
    global preprocessing_process

    if _status == True and preprocess_state == False:
        preprocess_state = True
        preprocessing_process = Process(
            name='preprocessing',
            target=preprocessing.run,
            args=(sig_preprocess,)
        )
        sig_preprocess.clear()
        preprocessing_process.start()
    
        return True
    
    elif _status == False and preprocess_state == True:
        preprocess_state = False
        sig_preprocess.set()
        
        return True

    elif _status == "info" and preprocess_state == True:
        _result = {
            "name" : preprocessing_process.name,
            "pid" : preprocessing_process.pid,
            "alive" : preprocessing_process.is_alive()
        }

        return _result

    else:
        return False

def trend_management(_status):
    """To manage trend process
    Trend process start if _status True and stop all if _status false

    Args:
        _status (bool): True is start, False is stop

    Returns:
        bool: True is start or stop, False is nothing happen
    """
    global sig_trend
    global trend_state
    global trend_process

    if _status == True and trend_state == False:
        trend_state = True
        trend_process = Process(
            name='trend',
            target=trend.run,
            args=(sig_trend,)
        )
        sig_trend.clear()
        trend_process.start()

        return True

    elif _status == False and trend_state == True:
        trend_state = False
        sig_trend.set()

        return True

    elif _status == "info" and trend_state == True:
        _result = {
            "name" : trend_process.name,
            "pid" : trend_process.pid,
            "alive" : trend_process.is_alive()
        }

        return _result
    
    else:
        return False

_prediction_list = []
def prediction_management(_status, _model=None, _numb=None):
    global sig_prediction
    global prediction_state
    prediction_process = "prediction_process_{}"

    if _status == True and prediction_state == False:
        
        if _model == "bi":
            for i in range(_numb):
                globals()[prediction_process.format(i)] = Process(
                    name="prediction_bi",
                    target=prediction_bi.run,
                    args=(sig_prediction,)
                )
                globals()[prediction_process.format(i)].start()
                _prediction_list.append(globals()[prediction_process.format(i)])
            sig_prediction.clear()
            prediction_state = True

            return True

        elif _model == "conv2d":
            for i in range():
                globals()[prediction_process.format(i)] = Process(
                    name="prediction_conv2d",
                    target=prediction_conv2d.run,
                    args=(sig_prediction,)
                )
                globals()[prediction_process.format(i)].start()
                _prediction_list.append(globals()[prediction_process.format(i)])
            sig_prediction.clear()
            prediction_state = True

            return True
        
        else:
            return False

    elif _status == False and prediction_state == True:
        prediction_state = False
        sig_prediction.set()

        return True

    elif _status == "info" and prediction_state == True:
        _result = []
        print(len(_prediction_list))
        print(_prediction_list)
        for i in range(len(_prediction_list)):
            _result.append({
                "name" : _prediction_list[i].name,
                "pid" : _prediction_list[i].pid,
                "alive" : _prediction_list[i].is_alive()
            })

        return _result

    else:
        return False


app = Flask(__name__)
api = Api(app, version='0.1',
    title='AI Engine Server',
    description='AI Engine Server API',
    terms_url="/",
    contact="justinwk11@hncorp.world",
    license="Apache License 2.0"
)

model_api = api.namespace(
    name='processes of model',
    path='/model',
    description='Process of model API'
)

process_api = api.namespace(
    name='whole process',
    path='/process',
    description='Process API'
)

preprocess_api = api.namespace(
    name='preprocess',
    path='/process/preprocess',
    description='Preprocess API'
)
trend_api = api.namespace(
    name='trend',
    path='/process/trend',
    description='Trend API'
)
prediction_api = api.namespace(
    name='prediction',
    path='/process/prediction',
    description='Prediction API'
)

wrong_request_start = {
    'State' : 'Proccess Still running. please stop process first'
}

wrong_request_stop = {
    'State' : 'Nothing to stop process. please start process first'
}


@process_api.route('/start_all')
class ProcessStart(Resource):
    @process_api.doc('Whole Process Start')
    def get(self):
        _process_control = process_management(True)
        if _process_control == True:
            return {'State' : 'Whole Process Start'}
        else:
            return wrong_request_start

@process_api.route('/stop_all')
class ProcessStop(Resource):
    @process_api.doc('whole Process Stop')
    def get(self):
        _process_control = process_management(False)
        if _process_control == True:
            return {'State' : 'Whole Process Stop'}
        else:
            return wrong_request_stop

@process_api.route('/info_all')
class ProcessInfo(Resource):
    @process_api.doc('whole Process Info')
    def get(self):
        _process_control = process_management("info")
        return _process_control

@preprocess_api.route('/start')
class PreprocessStart(Resource):
    @preprocess_api.doc('Preprocessing Process Start')
    def get(self):
        _process_control = preprocess_management(True)
        if _process_control == True:
            return {'State' : 'Preprocess Start'}
        else:
            return wrong_request_start
        

@preprocess_api.route('/stop')
class PreprocessEnd(Resource):
    @preprocess_api.doc('Preprocessing Process Stop')
    def get(self):
        _process_control = preprocess_management(False)
        if _process_control == True:
            return {'State' : 'Preprocess Stop'}
        else:
            return wrong_request_stop


@preprocess_api.route('/info')
class PreprocessInfo(Resource):
    @preprocess_api.doc('Preprocessing Process Status')
    def get(self):
        _process_control = preprocess_management("info")

        return _process_control


@trend_api.route('/start')
class TrendStart(Resource):
    @trend_api.doc('Trend Process Start')
    def get(self):
        _process_control = trend_management(True)
        if _process_control == True:
            return {'State' : 'Trend start'}
        else:
            return wrong_request_start

@trend_api.route('/stop')
class TrendEnd(Resource):
    @trend_api.doc('Trend Process Stop')
    def get(self):
        _process_control = trend_management(False)
        if _process_control == True:
            return {'State' : 'Trend Stop'}
        else:
            return wrong_request_stop

@trend_api.route('/info')
class TrendInfo(Resource):
    @trend_api.doc('Trend Process Status')
    def get(self):
        _process_control = trend_management("info")
        return _process_control

@prediction_api.route('/<model>/<int:num_process>')
class PredicttionStart(Resource):
    @prediction_api.doc('Prediction Process Start')
    def get(self, model, num_process):
        _process_control = prediction_management(True, model, num_process)
        if _process_control == True:
            return {'State' : 'Prediction Start'}
        else:
            return wrong_request_start

@prediction_api.route('/stop')
class PredictionEnd(Resource):
    @prediction_api.doc('Prediction Process Stop')
    def get(self):
        _process_control = prediction_management(False)
        if _process_control == True:
            return {'State' : 'Prediction Stop'}
        else:
            return wrong_request_stop


@prediction_api.route('/info')
class PredictionInfo(Resource):
    @prediction_api.doc('Prediction Process Status')
    def get(self):
        _process_control = prediction_management("info")
        return _process_control


if __name__ == "__main__":
    preprocess_management(True)
    trend_management(True)
    prediction_management(True, "bi", 4)
    
    app.run(debug=False, host='9.8.100.151', port=8080)
