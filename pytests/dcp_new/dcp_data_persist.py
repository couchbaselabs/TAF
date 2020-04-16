""" Functions for editing JSON format files which deal with log updates,
    to persist uuids and seq_nos for each vbucket.
    Currently stored within folder called 'logs'
    Potential scope to save more data about vbucket if needed
"""

import json
import os


class LogData(object):
    """ Class to control instance of data for vbuckets
        If internal use is requested, 'None' should be passed into dirpath"""

    def __init__(self, dirpath, vbucket_list, keep_logs):
        self.dictstore = {}
        if dirpath is not None:
            # Create with external file logging
            self.external = True
            self.path = os.path.join(dirpath, os.path.normpath('logs/'))
            if keep_logs:
                reset_list = []
                preset_list = []
                for vb in vbucket_list:
                    if os.path.exists(self.get_path(vb)):
                        preset_list.append(vb)
                    else:
                        reset_list.append(vb)
                self.setup_log_preset(preset_list)
            else:
                reset_list = vbucket_list
            self.reset(reset_list)
        else:
            self.external = False


    def setup_log_preset(self, vb_list):
        """ Used when --keep-logs is triggered, to move external data to dictstore """
        external_data = self.get_all_external(vb_list)
        for key in external_data.keys():
            self.dictstore[str(key)] = external_data[key]

    def get_path(self, vb):
        """ Retrieves path to log file for inputted virtual bucket number """
        # Make directory if it doesn't exist
        if self.external:
            fullpath = os.path.join(self.path, os.path.normpath('{}.json'.format(vb)))
            dirname = os.path.dirname(fullpath)
            if dirname and not os.path.exists(dirname):
                os.mkdir(dirname)
            elif os.path.isdir(dirname):
                pass  # Confirms that directory exists
            else:
                raise IOError("Cannot create directory inside a file")

            return fullpath

        else:
            raise RuntimeError('LogData specified as internal, no external path')

    def reset(self, vb_list):
        """ Clears/makes files for list of virtual bucket numbers"""
        for vb in vb_list:
            self.dictstore[str(vb)] = {}
            if self.external:
                path_string = self.get_path(vb)
                with open(path_string, 'w') as f:
                    json.dump({}, f)

    def upsert_failover(self, vb, failover_log):
        """ Insert / update failover log """
        if str(vb) in self.dictstore.keys():
            self.dictstore[str(vb)]['failover_log'] = failover_log
        else:
            self.dictstore[str(vb)] = {'failover_log': failover_log}

        if self.external:
            path_string = self.get_path(vb)

            with open(path_string, 'r') as vb_log:
                data = json.load(vb_log)

            data['failover_log'] = failover_log

            with open(path_string, 'w') as vb_log:
                json.dump(data, vb_log)

    def upsert_sequence_no(self, vb, seq_no):
        """ Insert / update sequence number, and move old sequence number to appropriate list """
        if str(vb) in self.dictstore.keys():
            if 'seq_no' in self.dictstore[str(vb)].keys():
                if 'old_seq_no' in self.dictstore[str(vb)].keys():
                    old_seq_no = self.dictstore[str(vb)]['old_seq_no']
                else:
                    old_seq_no = []
                old_seq_no.append(self.dictstore[str(vb)]['seq_no'])
                self.dictstore[str(vb)]['old_seq_no'] = old_seq_no
            self.dictstore[str(vb)]['seq_no'] = seq_no
        else:
            self.dictstore[str(vb)] = {'seq_no': seq_no}

    def push_sequence_no(self, vb):
        """ Push sequence number and old sequence number to external JSON files """
        if self.external:
            path_string = self.get_path(vb)

            with open(path_string, 'r') as vb_log:
                data = json.load(vb_log)

            data['old_seq_no'] = self.dictstore[str(vb)].get('old_seq_no')

            data['seq_no'] = self.dictstore[str(vb)].get('seq_no')

            with open(path_string, 'w') as vb_log:
                json.dump(data, vb_log)

    def get_all(self, vb_list):
        """ Return a dictionary where keys are vbuckets and the data is the total JSON for that vbucket """
        read_dict = {}
        for vb in vb_list:
            if str(vb) in self.dictstore.keys():
                read_dict[str(vb)] = self.dictstore[str(vb)]

        return read_dict

    def get_all_external(self, vb_list):
        read_dict = {}
        if self.external:
            for vb in vb_list:
                path_string = self.get_path(vb)
                if os.path.exists(path_string):
                    with open(path_string, 'r') as vb_log:
                        data = json.load(vb_log)
                    read_dict[str(vb)] = data
            return read_dict
        else:
            raise IOError("No external files setup")

    def get_seq_nos(self, vb_list):
        """ Return a dictionary where keys are vbuckets and the data is the sequence number """
        read_dict = {}
        for vb in vb_list:
            if str(vb) in self.dictstore:
                read_dict[str(vb)] = self.dictstore[str(vb)].get('seq_no')

        return read_dict

    def get_failover_logs(self, vb_list):
        """ Return a dictionary where keys are vbuckets and the data is the failover log list """
        read_dict = {}
        for vb in vb_list:
            if str(vb) in self.dictstore:
                read_dict[str(vb)] = self.dictstore[str(vb)].get('failover_log')

        return read_dict
