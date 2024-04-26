import errno
import os
import requests
import yaml
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
    def start_sirius(taf_path, port=4000):
        fp = open("logs/sirius.log", "a")

        print("Building sirius")
        cmd = ["/bin/sh", "-c",
               f"cd {taf_path}/sirius ; make clean ; make build"]

        process = Popen(cmd, stdout=fp, stderr=fp)
        process.communicate()
        fp.close()

        print(f"Starting Sirius client on port '{port}'")
        fp = open("logs/sirius.log", "a")
        cmd = [f"{taf_path}/sirius/sirius", "-port", port]
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

    @staticmethod
    def start_sirius_docker(port=4000):
        docker_file_path = os.path.join(os.getcwd(), "sirius",
                                        "docker-compose.yaml")
        if os.path.exists(docker_file_path):
            with open(docker_file_path) as stream:
                try:
                    docker_file_data = yaml.safe_load(stream=stream)
                except Exception:
                    raise Exception

            unique_name = f"sirius_{port}"
            docker_file_data["services"]["sirius"]["ports"] = [f"{port}:4000"]
            docker_file_data["services"]["sirius"][
                "container_name"] = unique_name
            docker_file_data["services"][unique_name] = \
                docker_file_data["services"]["sirius"]
            docker_file_data["services"].pop("sirius")
            docker_file_data["networks"]['default']['name'] = unique_name

            with open(docker_file_path, 'w') as outfile:
                yaml.safe_dump(docker_file_data, outfile,
                               default_flow_style=False)

            fp = open("logs/sirius.log", "w")
            cmd = ["/bin/sh", "-c", "cd sirius ; make clean_deploy"]
            process = Popen(cmd, stdout=fp, stderr=fp)
            process.communicate()
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT),
                                    docker_file_path)

    @classmethod
    def stop_sirius_docker(cls):
        fp = open("logs/sirius.log", "a")
        cmd = ["/bin/sh", "-c", "cd sirius ; make down"]
        process = Popen(cmd, stdout=fp, stderr=fp)
        process.communicate()
