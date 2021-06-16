import asyncio
import aiohttp

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
        try:
            async with session.get(_request_info + _request_param) as response:
                if response.status == 200:
                    result = await response.read()
                else:
                    print(response.status)
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
        try:
            async with session.get(_request_info + _request_param) as response:
                result = await response.read()
        except aiohttp.ClientConnectionError as e:
            print("loss_connection error", str(e))