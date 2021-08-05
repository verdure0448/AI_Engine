
from time import sleep
import confluent_kafka as kafka
from confluent_kafka import Consumer, KafkaError
from multiprocessing import Process, Queue
import q_subscribe_ps
import q_predict_ps
import copy

_convert_t_code = ['T1010','T1100']
def append_datas(p_list, msg):
    split_decoded_msg = str(msg.decode('utf-8')).split(",")
    load = float(split_decoded_msg[4])
    scale_load = load
    if scale_load < 200:
        scale_load = 0
    t_code = split_decoded_msg[8]
    if t_code in _convert_t_code:
        t_code = 'T0000'

    # op-code, time, load, t-code, scale-load, sn
    # print('append => {0}, {1}, {2}, {3}, {4}'.format(split_decoded_msg[0], split_decoded_msg[1], split_decoded_msg[4], t_code, scale_load, split_decoded_msg[14]))
    p_list.append([split_decoded_msg[0], split_decoded_msg[1], load, t_code, scale_load, split_decoded_msg[14]])

    return p_list

def analyze_data(p_list):
    global data_status

    if len(p_list) < 20: # 분석데이터 최소 사이즈 미달
        # if p_list[0][3] is not 'T0000':
        #     p_list = []
        return p_list


    if len(p_list) > 2500: # 최대사이즈 초과시
        p_list = p_list[-2500:] # 과거 데이터 삭제

    if data_status == 0: # 초기 상태에서 가공 시작점 찾기
        for i in range(len(p_list)):
            if p_list[i][3]  == 'T8080' : # T8080 첫출현 시작 이전 20개의 데이터가 있으면
                for j in range(i):
                    if p_list[i-j][4] == 0: #scale_load가 0
                        p_list = p_list[i-j:] # 가공 시작 이전 데이터 제거
                        data_status = 1   # 가공 시작 상태
                        print('[DEBUG] 시작점 발견.')
                        print('[DEBUG] 데이터 사이즈 {0}  시작점 idx {1}'.format(len(p_list), i-j))
                        print('[DEBUG] p_list[0] {0}'.format(p_list[0]))
                
                        break
                if data_status != 1:
                    # raise Exception('시작점 데이터 예외 발생.')
                    p_list = [] # 상기 브레이크 조건에 맞지 않으면 배열 초기화
                    return p_list

    elif data_status == 1: # 가공 시작상태에서 종료점 찾기
        if p_list[-1][3] == 'T0000':
            print('[DEBUG] 종료코드 발견 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            data_status = 2
        elif len(p_list) > 2000:
            print('[DEBUG] 제품생산 C/T Over.')
            p_list = []
            # raise Exception('[DEBUG] 제품생산 C/T Over')
    elif data_status == 2: # 종료코드 출현 후 종료점 찾기
        if p_list[-1][3] != 'T0000': # 종료코드 이외의 값이 존재할 경우 예외처리
            print('[DEBUG] 종료점 찾기 실패')
            data_status = 0
            p_list = []
            print('[DEBUG] 종료점 데이터 예외 발생.')
            # raise Exception('[DEBUG] 종료점 데이터 예외 발생.')
        else:
            zero_count = 0
            for i in range(20): # 종료(작업없음)코드에서 부하값 0 출현 카운터
                if p_list[-(i+1)][3] == 'T0000' and p_list[-(i+1)][4] == 0:
                    zero_count += 1 
            if zero_count == 20:# 0 부하가 20개 이상 출현시에 종료점으로 OK
                print('[DEBUG] 1 Cycle 제품 가공 부하값 추출 성공 C/T: ', len(p_list[:-20]))
                copy_data = copy.deepcopy(p_list[:-20])
                # 제품 품질 판정 처리 프로세스 호출
                p_predict = Process(name='Quality predict', target=q_predict_ps.process, args=(copy_data,))
                p_predict.start()

                data_status = 0 # 상태 초기화
                p_list = [] # 버프 초기화
            
    else:
        print('[DEBUG] 데이터 상태 처리 없음')
    
    return p_list

if __name__ == "__main__":
    q = Queue()
    p_sbuscribe = Process(name='Quality sbuscribe', target=q_subscribe_ps.process, args=(q,))
    p_sbuscribe.start()
    # ps_sbuscribe.join()
    data_list = []
    
    global data_status # 0: 초기, 1: 가공시작, 2: 가공종료
    global s_tcode_idx
    global data_idx

    data_status = 0
    data_idx = -1
    s_tcode_idx = -1

    try:
        while True:
            if q.qsize() > 0:
                # print('Get queue: {0}'.format(q.get()))
                data_list = append_datas(data_list, q.get())
                data_list = analyze_data(data_list)
                # print('Data list size: {0}'.format(len(data_list)))
            else:
                sleep(0.01)
    except Exception as e:
        print(e)
        raise e
        # pass
    finally:
        p_sbuscribe.kill()
        

   
