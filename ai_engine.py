import module.process.preprocessing as preprocessing
import module.process.trend as trend
import module.process.prediction as prediction

from multiprocessing import Process, Event

from flask import Flask
from flask_restplus import Api, Resource, fields

class Engine:
    def __init__(self):
        self.sig_preprocess_stop = Event()
        self.sig_trend_process_stop = Event()
        self.sig_prediction_process_stop = Event()

        self.preprocessing_process = Process(
            name='preprocessing',
            target=preprocessing.run,
            args=(self.sig_preprocess_stop,)
        )

        self.trend_process = Process(
            name='trend',
            target=trend.run,
            args=(self.sig_trend_process_stop,)
        )

        self.prediction_process = Process(
            name='prediction',
            target=prediction.run,
            args=(self.sig_prediction_process_stop,)
        )

    def process_start(self, _process):
        if _process == 'preprocessing':
            self.preprocessing_process.start()
        elif _process == 'trend':
            self.trend_process.start()
        elif _process == 'prediction':
            self.prediction_process.start()
        elif _process == 'all':
            self.preprocessing_process.start()
            self.trend_process.start()
            self.prediction_process.start()
        else:
            print("select undefined process")

    def process_stop(self, _process):
        if _process == 'preprocessing':
            self.sig_preprocess_stop.set()
        elif _process == 'trend':
            self.sig_trend_process_stop.set()
        elif _process == 'prediction':
            self.sig_prediction_process_stop.set()
        elif _process == 'all':
            self.sig_preprocess_stop.set()
            self.sig_trend_process_stop.set()
            self.sig_prediction_process_stop.set()
        else:
            print("select undefined process signal")

app = Flask(__name__)
api = Api(app, version='0.1',
    title='AI Engine Server',
    description='AI Engine Server API',
    terms_url="/",
    contact="justinwk11@hncorp.world",
    license="Apache License 2.0"
)

sig_process_stop_all = None

sig_preprocess_stop = None
sig_trend_stop = None
sig_prediction_stop = None

preprocess_api = api.namespace('preprocess', description='Preprocess API')
trend_api = api.namespace('trend', description='Trend API')
prediction_api = api.namespace('prediction', decsription='Prediction API')

engine = Engine()

@preprocess_api.route('/start')
class PreprocessStart(Resource):
    @preprocess_api.doc('Preprocessing Process Start')
    def get(self):
        engine.process_start('preprocessing')

        return {'State' : 'Preprocess start'}

@preprocess_api.route('/stop')
class PreprocessEnd(Resource):
    @preprocess_api.doc('Preprocessing Process Stop')
    def get(self):
        engine.process_stop('preprocessing')

        return {'State' : 'Preprocess Stop'}

@trend_api.route('/start')
class TrendStart(Resource):
    @trend_api.doc('Trend Process Start')
    def get(self):
        engine.process_start('trend')

        return {'State' : 'Trend start'}

@trend_api.route('/stop')
class TrendEnd(Resource):
    @trend_api.doc('Trend Process Start')
    def get(self):
        engine.process_stop('trend')

        return {'State' : 'Trend Stop'}

@prediction_api.route('/start')
class PredictionStart(Resource):
    @prediction_api.doc('Prediction Process Start')
    def get(self):
        engine.process_start('prediction')

        return {'State' : 'Prediction start'}

@prediction_api.route('/stop')
class PredictionEnd(Resource):
    @prediction_api.doc('Prediction Process Stop')
    def get(self):
        engine.process_stop('prediction')

        return {'State' : 'Prediction Stop'}


if __name__ == "__main__":
    engine = Engine()
    # engine.process_start('all')
    app.run(debug=True, host='9.8.100.151', port=8080)