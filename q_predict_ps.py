from time import sleep
import librosa
import librosa.display
import numpy as np
import matplotlib.pyplot as plt

from tensorflow.keras import models
from tensorflow.keras.models import Model, load_model
from tensorflow.keras.preprocessing import image
import requests

from PIL import Image
'''
품질 예측 처리 시작 전에 업무서비스에 처리 시작을 알리는 HTTP request
 args
    opcode : 공정코드
    sn : 제품 시리얼번호
 return
'''
def http_request_start(opcode, sn):
    response = requests.get('http://9.8.100.153:8082/quality/start', params={'opcode':opcode, 'sn': sn}, timeout=3)
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
def http_request_end(opcode, sn, predict, acc, file_path):
    url = 'http://9.8.100.153:8082/quality/end?opcode={0}&sn={1}&predict={2}&acc={3}'.format(opcode, sn, predict, acc)
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

def check_image_size(img_path):
    img = Image.open(img_path)
    print('image size ====> ', img.size)
    new_img = img.resize((558, 271), resample=Image.LANCZOS)
    new_img.save(img_path)
'''
품질예측 메인 프로세스 처리
'''
def process(datas):
    print('Start predict_process.')
    opcode = datas[0][0] # 대표 공정코드 
    sn = datas[0][5]
    http_request_start(opcode, sn)
    # 데이터 전처리
    # input_signal = [float(i) for i in data[2]]
    input_signal = [data[2] for data in datas]
    resample_data = librosa.resample(np.array(input_signal, dtype=np.float), len(input_signal), 10240)

    file_name = 'img/%s/item-%s.jpeg'%(opcode, sn)
    create_stft_image(signal=resample_data, file_name=file_name)
    check_image_size(file_name)
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
    predict_idx = np.argmax(preds[0])
    print('Predicted: ', preds)
    print('Predicted result ==> {0} : {1}'.format(predict_idx, preds[0][predict_idx]))
    http_request_end(opcode, sn, predict_idx, preds[0][predict_idx], file_name)
    print('End predict_process.')
    
if __name__ == "__main__":
    data = []
    process(data)