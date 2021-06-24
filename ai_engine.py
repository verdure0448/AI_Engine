import module.process.preprocessing as preprocessing
import module.process.trend as trend
import module.process.prediction as prediction

from multiprocessing import Process, Event

from flask import Flask
from flask_restplus import Api, Resource, fields


sig_preprocess = Event()
sig_trend = Event()
sig_prediction = Event()

preprocess_state = False
trend_state = False
prediction_state = False

def process_management(_status):
    global sig_preprocess, sig_trend, sig_prediction
    global preprocess_state, trend_state, prediction_state

    if _status == True and preprocess_state == False and trend_state == False and prediction_state == False:
        preprocess_management(True)
        trend_management(True)
        prediction_management(True)

        return True
    elif _status == False and preprocess_state == True and trend_state == True and prediction_state == True:
        preprocess_management(False)
        trend_management(False)
        prediction_management(False)

        return True

    else:
        return False

def preprocess_management(_status):
    global sig_preprocess
    global preprocess_state

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

    else:
        return False

def trend_management(_status):
    global sig_trend
    global trend_state

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
    
    else:
        return False

def prediction_management(_status):
    global sig_prediction
    global prediction_state

    if _status == True and prediction_state == False:
        prediction_state = True
        prediction_process = Process(
            name='prediction',
            target=prediction.run,
            args=(sig_prediction,)
        )
        sig_prediction.clear()
        prediction_process.start()

        return True

    elif _status == False and prediction_state == True:
        prediction_state = False
        sig_prediction.set()

        return True
    
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
    decsription='Prediction API'
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
        if _process_control != False:
            return {'State' : 'Whole Process Start'}
        else:
            return wrong_request_start

@process_api.route('/stop_all')
class ProcessStop(Resource):
    @process_api.doc('whole Process Stop')
    def get(self):
        _process_control = process_management(False)
        if _process_control != False:
            return {'State' : 'Whole Process Stop'}
        else:
            return wrong_request_stop

@preprocess_api.route('/start')
class PreprocessStart(Resource):
    @preprocess_api.doc('Preprocessing Process Start')
    def get(self):
        _process_control = preprocess_management(True)
        if _process_control != False:
            return {'State' : 'Preprocess Start'}
        else:
            return wrong_request_start
        

@preprocess_api.route('/stop')
class PreprocessEnd(Resource):
    @preprocess_api.doc('Preprocessing Process Stop')
    def get(self):
        _process_control = preprocess_management(False)
        if _process_control != False:
            return {'State' : 'Preprocess Stop'}
        else:
            return wrong_request_stop
        

@trend_api.route('/start')
class TrendStart(Resource):
    @trend_api.doc('Trend Process Start')
    def get(self):
        _process_control = trend_management(True)
        if _process_control != False:
            return {'State' : 'Trend start'}
        else:
            return wrong_request_start

@trend_api.route('/stop')
class TrendEnd(Resource):
    @trend_api.doc('Trend Process Stop')
    def get(self):
        _process_control = trend_management(False)
        if _process_control != False:
            return {'State' : 'Trend Stop'}
        else:
            return wrong_request_stop

@prediction_api.route('/start')
class PredictionStart(Resource):
    @prediction_api.doc('Prediction Process Start')
    def get(self):
        _process_control = prediction_management(True)
        if _process_control != False:
            return {'State' : 'Prediction start'}
        else:
            return wrong_request_start

@prediction_api.route('/stop')
class PredictionEnd(Resource):
    @prediction_api.doc('Prediction Process Stop')
    def get(self):
        _process_control = prediction_management(False)
        if _process_control != False:
            return {'State' : 'Prediction Stop'}
        else:
            return wrong_request_stop


if __name__ == "__main__":
    process_management(True)
    app.run(debug=False, host='9.8.100.151', port=8080)
