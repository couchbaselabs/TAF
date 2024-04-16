#!/usr/bin/env python
import getopt
import sys
import os
import time
from threading import Thread
from datetime import datetime
import subprocess
import platform

from shell_util.remote_connection import RemoteMachineShellConnection

import TestInput
from platform_constants.os_constants import Windows


def usage(error=None):
    print("""\
Syntax: collect_server_info.py [options]

Options
 -l  Lower case of L. For local run on windows only.  No need ini file.
 -i <file>        Path to .ini file containing cluster information.
 -p <key=val,...> Comma-separated key=value info.

Available keys:
 path=<file_path> The destination path you want to put your zipped 
 diag file

Example:
 collect_server_info.py -i cluster.ini -p path=/tmp/nosql
""")
    sys.exit(error)


def time_stamp():
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    hour = now.timetuple().tm_hour
    min = now.timetuple().tm_min
    date_time = "%s%02d%02d-%02d%02d" % (year, month, day, hour, min)
    return date_time


class couch_dbinfo_Runner(object):
    def __init__(self, server, path, local=False):
        self.server = server
        self.path = path
        self.local = local

    def run(self):
        file_name = "%s-%s-couch-dbinfo.txt" % (
            self.server.ip, time_stamp())
        if not self.local:
            remote_client = RemoteMachineShellConnection(self.server)
            print("Collecting dbinfo from %s\n" % self.server.ip)
            output, error = remote_client.execute_couch_dbinfo(
                file_name)
            print("\n".join(output))
            print("\n".join(error))

            user_path = "/home/"
            if remote_client.info.distribution_type.lower() == 'mac':
                user_path = "/Users/"
            else:
                if self.server.ssh_username == "root":
                    user_path = "/"

            remote_path = "%s%s" % (user_path, self.server.ssh_username)
            status = remote_client.file_exists(remote_path, file_name)
            if not status:
                raise Exception(
                    "%s doesn't exists on server" % file_name)
            status = remote_client.get_file(remote_path, file_name,
                                            "%s/%s" % (
                                                self.path, file_name))
            if status:
                print("Downloading dbinfo logs from %s" % self.server.ip)
            else:
                raise Exception("Fail to download db logs from %s"
                                % self.server.ip)
            remote_client.execute_command(
                "rm -f %s" % os.path.join(remote_path, file_name))
            remote_client.disconnect()


class cbcollectRunner(object):
    def __init__(self, server, path, local=False):
        self.server = server
        self.path = path
        self.local = local

    def run(self):
        file_name = "%s-%s-diag.zip" % (self.server.ip, time_stamp())
        if not self.local:
            remote_client = RemoteMachineShellConnection(self.server)
            print("Collecting logs from %s\n" % self.server.ip)
            output, error = remote_client.execute_cbcollect_info(
                file_name)
            print("\n".join(error))

            user_path = "/home/"
            if remote_client.info.distribution_type.lower() == 'mac':
                user_path = "/Users/"
            else:
                if self.server.ssh_username == "root":
                    user_path = "/"

            remote_path = "%s%s" % (user_path, self.server.ssh_username)
            status = remote_client.file_exists(remote_path, file_name)
            if not status:
                raise Exception(
                    "%s doesn't exists on server" % file_name)
            status = remote_client.get_file(remote_path, file_name,
                                            "%s/%s" % (
                                                self.path, file_name))
            if status:
                print("Downloading zipped logs from %s" % self.server.ip)
            else:
                raise Exception("Fail to download zipped logs from %s"
                                % self.server.ip)
            remote_client.execute_command(
                "rm -f %s" % os.path.join(remote_path, file_name))
            remote_client.disconnect()


def main():
    local = False
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hli:p', [])
        for o, a in opts:
            if o == "-h":
                usage()
            elif o == "-l":
                if platform.system() == "Windows":
                    print("*** windows os ***")
                    local = True
                else:
                    print("This option '-l' only works for local windows.")
                    sys.exit()
        if not local:
            input = TestInput.TestInputParser.get_test_input(sys.argv)
            if not input.servers:
                usage(
                    "ERROR: no servers specified. Please use the -i "
                    "parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError as error:
        usage("ERROR: " + str(error))

    if not local:
        file_path = input.param("path", ".")
        remotes = (cbcollectRunner(server, file_path, local) for server
                   in input.servers)

        remote_threads = [Thread(target=remote.run()) for remote in
                          remotes]
        for remote_thread in remote_threads:
            remote_thread.daemon = True
            remote_thread.start()
            run_time = 0
            while remote_thread.isAlive() and run_time < 1200:
                time.sleep(15)
                run_time += 15
                print("Waiting for another 15 seconds (time-out after 20mins)")
            if run_time == 1200:
                print("cbcollect_info hung on this node. Jumping to next node")
            print("collect info done")

        for remote_thread in remote_threads:
            remote_thread.join(120)
            if remote_thread.isAlive():
                raise Exception("cbcollect_info hung on remote node")
    else:
        file_name = "%s-%s-diag.zip" % ("local", time_stamp())
        cbcollect_command = Windows.COUCHBASE_BIN_PATH_RAW + "cbcollect_info.exe"
        result = subprocess.check_call([cbcollect_command, file_name])
        if result == 0:
            print("Log file name is \n %s" % file_name)
        else:
            print("Failed to collect log")


class MemInfoRunner(object):
    def __init__(self, server, local=False):
        self.server = server
        self.local = local
        self.succ = {}
        self.fail = []

    def _run(self, server):
        mem = None
        try:
            if not self.local:
                remote_client = RemoteMachineShellConnection(server)
                print("Collecting memory info from %s\n" % server.ip)
                remote_cmd = \
                    "sh -c 'if [[ \"$OSTYPE\" == \"darwin\"* ]]; " \
                    "then sysctl hw.memsize|grep -Eo [0-9]; " \
                    "else grep MemTotal /proc/meminfo|grep -Eo [0-9]; fi'"
                output, error = remote_client.execute_command(remote_cmd)
                print("\n".join(error))
                remote_client.disconnect()
                mem = int("".join(output))
        except Exception as e:
            self.fail.append((server.ip, e))
        else:
            if mem:
                self.succ[server.ip] = mem
            else:
                self.fail.append((server.ip, Exception("mem parse failed")))

    def run(self):
        if isinstance(self.server, list):
            meminfo_threads = []
            for server in self.server:
                meminfo_thread = Thread(target=self._run, args=(server,))
                meminfo_thread.start()
                meminfo_threads.append(meminfo_thread)
            for meminfo_thread in meminfo_threads:
                meminfo_thread.join()
        else:
            self._run(self.server)


if __name__ == "__main__":
    main()
