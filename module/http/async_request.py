import aiohttp
import json
from aiohttp.helpers import TOKEN
import requests


token = None

def get_token():
    global token
    with open("/home/rnd01/workspace/cnc_analyzer/config/config_request_token.json") as jsonFile:
        _conf = json.load(jsonFile)
    URL = 'http://9.8.100.153:8080/auth/realms/cyservice/protocol/openid-connect/token'
    data = {
        'client_id' : _conf['client_id'],
        'client_secret' : _conf['client_secret'],
        'grant_type' : _conf['grant_type'],
        'username' : _conf['username'],
        'password' : _conf['password']
    }
    res = requests.post(URL, data=data)
    if res.status_code == 200:
        token = res.text
        print("get token")

async def send_process_info(_url_head, _url_tail , _opcode, _start_time, _end_time, _cycle, _count):
    """Send request using HTTP protocol by asynchronously
    send a single cycle of process info

    Args:
        _url_head (str): domain to send request
        _url_tail (str): path to send request
        _opcode (str): operation code
        _start_time (str): start time for a single cycle
        _end_time (str): end time for a single cycle
        _cycle (float): the whole time for a single cycle
        _count (int): count is always 1
    """
    async with aiohttp.ClientSession() as session:
        _request_info = _url_head + _url_tail + "?"
        _request_param = "opCode=" + _opcode + "&startTime=" + _start_time + "&endTime=" + _end_time + "&cycleTime=" + str(_cycle) + "&count=" + str(_count)
        if token == None:
            get_token()
        try:
            async with session.get(_request_info + _request_param, headers={'token':token}) as response:
                if response.status == 200:
                    result = await response.read()
                    _decode = json.loads(result.decode('utf-8'))
                    print(response.status, _decode)
                        
        except aiohttp.ClientConnectionError as e:
            print("process_connection error", str(e))


async def send_loss_info(_url_head, _url_tail, _loss):
    """Send request using HTTP protocol by asynchronously
    send a loss rate

    Args:
        _url_head (str): domain to send request
        _url_tail (str): path to send request
        _loss (float64): result of mean squared error
    """
    async with aiohttp.ClientSession() as session:
        _request_info = _url_head + _url_tail + "?"
        _request_param = "loss=" + str(_loss)
        if token == None:
            get_token()        
        try:
            async with session.get(_request_info + _request_param, headers={'token':token}) as response:
                result = await response.read()
                #print(result)
                #_decode = await json.loads(result.decode('utf-8'))
                #print(response.status, _decode)

                # if _decode['errormessage'] == 'oauth.v2.TokenNotFound':
                #     _token = get_token()
                #     URL = "http://9.8.100.153:8081" + "/main/real-time-loss" + "?" + "loss=" + "0.001"
                #     res = requests.get(URL, headers={'token':_token})
                #     print(res)

                # if response.status == 200:
                #     print(result)
                # elif response.status == 401:
                #     print(result)
        except aiohttp.ClientConnectionError as e:
            print("loss_connection error", str(e))
