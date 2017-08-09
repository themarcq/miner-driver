#!/usr/bin/python3
from datetime import datetime, timedelta
import socket
import json
import time
import requests
import sys
import threading


MINER_DATA_REQUEST = json.dumps({"id":0,"jsonrpc":"2.0","method":"miner_getstat1"})


class Netcat:
    """ Python 'netcat like' module """

    def __init__(self, ip, port):
        self.buff = ""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((ip, port))

    def read(self, length = 1024):
        """ Read 1024 bytes off the socket """
        return self.socket.recv(length)

    def read_until(self, data):
        """ Read data into the buffer until we have data """
        while not data in self.buff:
            self.buff += self.socket.recv(1024)

        pos = self.buff.find(data)
        rval = self.buff[:pos + len(data)]
        self.buff = self.buff[pos + len(data):]

        return rval

    def write(self, data):
        self.socket.send(data)

    def close(self):
        self.socket.close()


class HubConnector():
    def __init__(self, address, token):
        self.address = address
        self.headers = self._get_headers(token)

    def post(self, endpoint, data):
        try:
            req = requests.post(
                "{}/{}".format(self.address, endpoint),
                headers=self.headers,
                json=data,
                timeout=0.5
            )
        except:
            ...
        else:
            if req.status_code // 100 != 2:
                req.raise_for_status()

    def _get_headers(self, token):
        return {
            'Content-Type': 'application/json',
            'Authorization': 'Token {token}'.format(token=token)
        }


class Miner():
    ERRORS_ENDPOINT = "api/farms/errors/"
    STATS_ENDPOINT = "api/farms/stats/"
    def __init__(self, ip, port, index):
        self.ip = ip
        self.port = port
        self.index = index

    def save(self, result, con):
        if type(result) is dict:
            data = {
                'worker_id': self.index,
                'error': result['error']
            }
            print('{}: ERROR ({})'.format(
                data['worker_id'],
                str(data['error'])
            ))
            sys.stdout.flush()
            try:
                con.post(self.ERRORS_ENDPOINT, data)
            except requests.HTTPError as e:
                print('ERROR - could not send an error')
                sys.stdout.flush()
                debug_http_error(e)
        elif type(result) is list:
            hashrate, shares, rejected_shares = result[2].split(';')
            alt_hashrate, alt_shares, alt_rejected_shares = result[4].split(';')
            gpus = []
            hashrates = result[3].split(';')
            alt_hashrates = result[5].split(';')
            health_stats = result[6].split(';')
            for i in range(len(hashrates)):
                gpus.append({
                    'hashrate': hashrates[i],
                    'alt_hashrate': alt_hashrates[i] if alt_hashrates[i] != 'off' else 0,
                    'temperature': health_stats[i*2],
                    'fan_speed': health_stats[i*2+1]
                })
            data = {
                'worker_id': self.index,
                'uptime': max(int(result[1]), 0),
                'total_hashrate': hashrate,
                'total_alt_hashrate': alt_hashrate if alt_hashrate != 'off' else 0,
                'shares': shares,
                'rejected_shares': rejected_shares,
                'gpu_stats': gpus,
            }
            print("{}: {}H/s".format(
                data['worker_id'],
                data['total_hashrate']
            ))
            sys.stdout.flush()
            try:
                con.post(self.STATS_ENDPOINT, data)
            except requests.HTTPError as e:
                print('ERROR - could not send statistics')
                debug_http_error(e)


def debug_http_error(e):
    data = "API connection error ({code} - {reason})\nresponse: {response}\nheaders: {headers}".format(
        code=e.response.status_code,
        reason=e.response.reason,
        response=e.response.text,
        headers=e.request.headers
    )
    print(data)
    sys.stdout.flush()


def load_config(path):
    with open(path) as data_file:
        config_json = json.load(data_file)
    return config_json


def download_and_save_miner_data(miner, connector):
    try:
        nc = Netcat(miner.ip, miner.port)
        nc.write(bytes(MINER_DATA_REQUEST, 'utf-8'))
        result = json.loads(nc.read().decode('utf-8'))
    except Exception as e:
        result = {"error": e}
    else:
        nc.close()
    if not result['error']:
        result = result['result']
    miner.save(result, connector)


def wait_till_full_minute():
    time.sleep(
        60 - int(datetime.now().timestamp())%60 + 5
    )


def main(config_path):
    print('Initializing...')
    sys.stdout.flush()
    config = load_config(config_path)
    connector = HubConnector(**config['database'])
    miners = {o['index']: Miner(**o) for o in config['miners']}
    socket.setdefaulttimeout(10)

    while True:
        wait_till_full_minute()
        print('Probing...')
        sys.stdout.flush()
        for index, miner in miners.items():
            t = threading.Thread(target=download_and_save_miner_data, args=(miner, connector))
            t.start()


if __name__ == '__main__':
    config_path = "config.json"
    params = iter(sys.argv[1:])
    for param in params:
        if param in ['-c', '--config']:
            config_path = next(params, '')
        if param in ['-o', '--output']:
            sys.stdout = open(next(params, ''), 'w')
    main(config_path)
