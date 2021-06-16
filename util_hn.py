from datetime import datetime


def delivery_report(err, msg):
    """

    Args:
        err (): 
        msg (): 

    """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


def time_to_strtime(_time):
    """

    Args:
        _time (): 
    Returns:

    """
    _float_time = float(_time)/1e3
    dt = datetime.fromtimestamp(_float_time)

    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')