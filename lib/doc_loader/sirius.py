import os
import requests
from signal import SIGTERM
from subprocess import Popen

from common_lib import sleep


class SiriusClient(object):
    __running_process = None

    @staticmethod
    def get_running_pid():
        return SiriusClient.__running_process

    @staticmethod
    def is_sirius_online(url):
        for i in range(5):
            try:
                response = requests.get(url=url + "/check-online")
                if response.status_code == 200:
                    return True
                sleep(20)
            except Exception as e:
                print(str(e))
        return False

    @staticmethod
    def start_sirius(port=4000):
        print(f"Starting Sirius client on port '{port}'")
        cmd = ["/bin/sh", "-c",
               f"SIRIUS_PORT={port} ; export SIRIUS_PORT ; "
               "cd sirius ; make build ; make clean ; "
               "make run"]
        fp = open("logs/sirius.log", "w")

        SiriusClient.__running_process = Popen(cmd, stdout=fp, stderr=fp)

    @staticmethod
    def terminate_sirius():
        if SiriusClient.__running_process:
            pid = SiriusClient.__running_process.pid
            print(f"Killing Sirius pid '{pid}'")
            os.kill(pid, SIGTERM)
            SiriusClient.__running_process.communicate()
        SiriusClient.__running_process = None

    def __init__(self):
        pass

    def clear_test_information(self, base_urls, identifier_token):
        for base_url in base_urls.split("|"):
            if not self.is_sirius_online(base_url):
                raise Exception("sirius is not online")
            headers = {'Content-Type': 'application/json'}
            json_data_request = {'identifierToken': identifier_token}

            exception = None
            for i in range(5):
                try:
                    path = "/clear_data"
                    response = requests.post(url=base_url + path,
                                             headers=headers,
                                             json=json_data_request)
                    response_data = response.json()
                    print("cleaning", response_data)
                    if not response_data["error"]:
                        print(response_data)
                    return
                except Exception as e:
                    print(str(e))
                    exception = e
            raise exception
