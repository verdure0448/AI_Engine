import aiohttp
import json
from aiohttp.helpers import TOKEN
import requests
import time


token = None
refresh_token = None
_conf = None
with open("/home/rnd01/workspace/cnc_analyzer/config/config_request_token.json") as jsonFile:
        _conf = json.load(jsonFile)

def get_token():
    """Get token from auth server to send loss & process info
    """
    print("get token ", time.strftime('%c', time.localtime((time.time()))))
    global token
    global refresh_token

    URL = _conf['auth_server']
    _data = {
        'client_id' : _conf['client_id'],
        'client_secret' : _conf['client_secret'],
        'grant_type' : _conf['grant_password'],
        'username' : _conf['username'],
        'password' : _conf['password']
    }
    res = requests.post(URL, data=_data)
    if res.status_code == 200:
        token = res.text
        refresh_token = res.json()['refresh_token']

def get_refresh_token():
    """Get refresh token from auth server when token authentication timeout
    """
    global token
    global refresh_token

    URL = _conf['auth_server']
    _data = {
        'client_id' : _conf['client_id'],
        'client_secret' : _conf['client_secret'],
        'grant_type' : _conf['grant_refresh'],
        'refresh_token' : refresh_token
    }
    res = requests.post(URL, data=_data)
    if res.status_code == 200:
        token = res.text
        refresh_token = res.json()['refresh_token']

def get_new_token():
    global token

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
                elif response.status == 401:
                    result = await response.read()
                    _decode = json.loads(result.decode('utf-8'))
                    if _decode['hnerrorcode'] == 404:
                        get_refresh_token()
                else:
                    print("unknown error")
                        
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
                if response.status == 200:
                    result = await response.read()
                elif response.status == 401:
                    result = await response.read()
                    _decode = json.loads(result.decode('utf-8'))
                    if _decode['hnerrorcode'] == 404:
                        get_refresh_token()
                else:
                    print("unknown error")

        except aiohttp.ClientConnectionError as e:
            print("loss_connection error", str(e))
