#!/usr/bin/python

import sys
from argparse import ArgumentParser
from threading import Thread
from datetime import datetime
import uuid


sys.path = [".", "lib", "couchbase_utils",
            "platform_utils", "platform_utils/ssh_util",
            "connections", "constants", "py_constants"] + sys.path
from ssh_util.shell_util.remote_connection import RemoteMachineShellConnection
import TestInput
import logging.config

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()


def usage(error=None):
    print("""\
Syntax: ssh.py [options] [command]

Options:
 -p <key=val,...> Comma-separated key=value info.
 -i <file>        Path to .ini file containing cluster information.

Available keys:
 script=<file>           Local script to run
 parallel=true           Run the command in parallel on all machines

Examples:
 ssh.py -i /tmp/ubuntu.ini -p script=/tmp/set_date.sh
 ssh.py -i /tmp/ubuntu.ini -p parallel=false ls -l /tmp/core*
""")
    sys.exit(error)


class CommandRunner(object):
    def __init__(self, server, command):
        self.server = server
        self.command = command

    def run(self):
        remote_client = RemoteMachineShellConnection(self.server)
        output, error = remote_client.execute_command(self.command)
        print(f"{self.server.ip} - '{self.command}' :: Output: {output}, Error: {error}")
        remote_client.disconnect()


class ScriptRunner(object):
    def __init__(self, server, script):
        self.server = server
        with open(script) as  f:
            self.script_content = f.read()
        self.script_name = "/tmp/" + str(uuid.uuid4())

    def run(self):
        remote_client = RemoteMachineShellConnection(self.server)
        remote_client.create_file(self.script_name, self.script_content)
        output, error = remote_client.execute_command(
            "chmod 777 {0} ; {0} ; rm -f {0}".format(self.script_name))
        print(self.server.ip)
        print("\n".join(output))
        print("\n".join(error))
        remote_client.disconnect()


class RemoteJob(object):
    def sequential_remote(self, input):
        remotes = []
        params = input.test_params
        for server in input.servers:
            if "script" in params:
                remotes.append(ScriptRunner(server, params["script"]))
            if "command" in params:
                remotes.append(CommandRunner(server, params["command"]))

        for remote in remotes:
            try:
                remote.run()
            except Exception as ex:
                print("unable to complete the job: {0}".format(ex))

    def parallel_remote(self, input):
        remotes = []
        params = input.test_params
        for server in input.servers:
            if "script" in params:
                remotes.append(ScriptRunner(server, params["script"]))
            if "command" in params:
                remotes.append(CommandRunner(server, params["command"]))

        remote_threads = []
        for remote in remotes:
            remote_threads.append(Thread(target=remote.run))

        for remote_thread in remote_threads:
            remote_thread.start()

        for remote_thread in remote_threads:
            remote_thread.join()


def main():
    parser = ArgumentParser(description="ssh script")
    parser.add_argument("-i", "--ini", dest="ini", required=True,
                        help="Path to .ini file containing server "
                             "information,e.g -i tmp/local.ini")
    parser.add_argument("--command", dest="cmd", default=None,
                        help="Command to run on remote server")
    parser.add_argument("--script", dest="script", default=None,
                        help="Script to run remotely")
    parser.add_argument("--run_sequentially",
                        dest="run_sequentially", action="store_false",
                        default=False, help="Script to run remotely")
    parser.add_argument("--params", dest="params")
    options = parser.parse_args()
    t_input = TestInput.TestInputParser.get_test_input(options)
    if options.cmd:
        t_input.test_params["command"] = options.cmd
    if options.run_sequentially:
        RemoteJob().sequential_remote(t_input)
    else:
        RemoteJob().parallel_remote(t_input)


if __name__ == "__main__":
    main()
