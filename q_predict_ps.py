from time import sleep
import librosa
import numpy as np
import matplotlib.pyplot as plt

from tensorflow.keras import models
from tensorflow.keras.models import Model, load_model
from tensorflow.keras.preprocessing import image
import requests

'''
품질 예측 처리 시작 전에 업무서비스에 처리 시작을 알리는 HTTP request
 args
    opcode : 공정코드
    sn : 제품 시리얼번호
 return
'''
def http_request_start(opcode, sn):
    response = requests.get('url', params={'opcode':opcode, 'sn': sn}, timeout=3)
    print("http_request_start response code: ", response.status_code)
'''
품질 예측 완료 후에 업무서비스에 처리 완료와 결과(예측, 이미지)를 전송하는 HTTP request
 args
    opcode : 공정코드
    sn : 제품 시리얼번호
    predict : 품질예측 인덱스
    file_path : 품질특성 이미지파일 경로
 return
'''
def http_request_end(opcode, sn, predict, file_path):
    url = 'http://9.8.100.155:8080/quality/upload?opcode={0}&sn={1}&predict={2}'.format(opcode, sn, predict)
    files = {'media': open(file_path, 'rb')}
    response = requests.post(url, files=files, timeout=3)
    print("http_request_end response code: ", response.status_code)


'''
스핀들 부하 신호를 fft변환 처리하여 스펙토그램 이미지 추출
'''
def create_stft_image(signal=None, n_fft=2048, hop_length=32, win_length=1024, sr=100, file_name=None):
    stft_data = librosa.stft(signal, n_fft=n_fft, hop_length=hop_length, win_length=win_length)
    magnitude = np.abs(stft_data)
    log_spectrogram = librosa.amplitude_to_db(magnitude, ref=np.max)
    plt.figure(figsize=(8,4))
    librosa.display.specshow(log_spectrogram, sr=sr, hop_length=hop_length)
    plt.ylim(0, int(n_fft/2)+1)
    plt.axis('off')
    plt.savefig(fname=file_name, bbox_inches='tight', pad_inches=0, dpi=90)
    print('saved %s'%(file_name))
    plt.close()

'''
품질예측 메인 프로세스 처리
'''
def process(data):
    print('Start predict_process.')
    http_request_start()
    # 데이터 전처리
    resample_data = librosa.resample(data[2].astype(np.float32), len(data[2]), 10240)

    file_name = 'img/op10-3/item-%s.jpeg'%('xxxx')
    create_stft_image(signal=resample_data, file_name=file_name)

    model = load_model('/home/dhkang/workspace/git/cnc_analyzer/model/quality/quaility-model-0.999512-0.998438.h5')
    # model.summary()

    img = image.load_img(file_name)
    img_tensor = image.img_to_array(img)
    img_tensor = np.expand_dims(img_tensor, axis=0)
    img_tensor /= 255.
    print(img_tensor.shape)

    # plt.imshow(img_tensor[0])
    # plt.show()

    preds = model.predict(img_tensor)

    print('Predicted: ', preds)
    print('Predicted result == {0}'.format(np.argmax(preds[0])))
    http_request_end('url', 'sn', np.argmax(preds[0]), file_name)
    print('End predict_process.')
    
if __name__ == "__main__":
    data = []
    process(data)