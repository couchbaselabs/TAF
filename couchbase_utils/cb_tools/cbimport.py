from cb_tools.cb_tools_base import CbCmdBase
from Cb_constants import CbServer
import re


class CbImport(CbCmdBase):
    def __init__(self, shell_conn, username="Administrator",
                 password="password", no_ssl_verify=None):
        CbCmdBase.__init__(self, shell_conn, "cbimport",
                           username=username, password=password)
        if no_ssl_verify is None:
            no_ssl_verify = CbServer.use_https
        self.cli_flags = ""
        if no_ssl_verify:
            self.cli_flags += " --no-ssl-verify"

    """
    Method import csv data from a file into KV bucket
    :param bucket str Name of the bucket into which the data has to be imported
    :param file_path str path of the data file
    :param key_fixed_character str fixed set of character to be used to generate key
    :param key_row_value str Name of the field from the data file whose value will be used to generate the key
    :param key_custom_generator str Custom key generators, accepted values - MONO_INCR, UUID
    :param field_separator str Specifies the field separator to use when reading the dataset. 
    By default the separator is a comma.
    :param infer_types bool By default all values in a CSV files are interpreted as strings. 
    If infer types is set then cbimport will look at each value and decide whether it is 
    a string, integer, or boolean value and put the inferred type into the document.
    """
    def import_cvs_data_from_file_into_bucket(
            self, bucket, file_path, key_fixed_character=None,
            key_row_value=None, key_custom_generator=None, field_separator=None,
            infer_types=False, no_of_threads=0):
        key = ""
        # If no key generator value is passed then use the custom mono_incr generator by default
        if key_fixed_character is None and key_row_value is None and key_custom_generator is None:
            key = "#MONO_INCR#"

        if key_fixed_character:
            key = key_fixed_character

        if key_row_value:
            if key:
                key += "::%{0}%".format(key_row_value)
            else:
                key = "%{0}%".format(key_row_value)

        if key_custom_generator:
            if key:
                key += "::#{0}#".format(key_custom_generator)
            else:
                key = "#{0}#".format(key_custom_generator)

        cmd = "%s csv --cluster %s:%s -u %s -p %s -b %s -d file://%s -g key::%s" \
              % (self.cbstatCmd, "localhost", self.port,
                 self.username, self.password, bucket, file_path, key)

        if infer_types:
            cmd += " --infer-types"

        if field_separator:
            cmd += " --field-separator '%s'" % field_separator

        if no_of_threads:
            cmd += " --threads {0}".format(no_of_threads)

        cmd += self.cli_flags
        output, error = self._execute_cmd(cmd)
        if len(error) != 0:
            raise Exception(str(error))
        elif "failed" in output[0]:
            raise Exception(str(output))
        else:
            for line in output:
                match = re.search(r"Documents failed:\s*([0-9]*)", line)
                if match:
                    if int(match.group(1)) > 0:
                        raise Exception(line)