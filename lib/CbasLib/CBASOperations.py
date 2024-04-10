"""
Created on Oct 24, 2017

@author: riteshagarwal
"""

import mode
import json
import numbers

if mode.java:
    from CbasLib.CBASOperations_JavaSDK import CBASHelper as CbasLib
elif mode.cli:
    from CbasLib.CBASOperations_CLI import CBASHelper as CbasLib
else:
    from CbasLib.CBASOperations_Rest import CBASHelper as CbasLib


class CBASHelper(CbasLib):

    @staticmethod
    def format_name(*args):
        """
        Enclose the name in `` if the name consist of - or starts with a number.
        """
        full_name = list()
        for name in args:
            if name:
                for _ in name.split("."):
                    _ = _.strip("`")
                    if _[0].isdigit() or ("-" in _):
                        full_name.append("`{0}`".format(_))
                    else:
                        full_name.append(_)
        return '.'.join(full_name)

    @staticmethod
    def unformat_name(*args):
        '''
        Strips the name of ``
        '''
        full_name = list()
        for name in args:
            for _ in name.split("."):
                _ = _.replace("`", "")
                full_name.append(_)
        return '.'.join(full_name)

    @staticmethod
    def metadata_format(name):
        return "/".join([_.replace("`", "") for _ in name.split(".")])

    @staticmethod
    def get_json(content="", json_data=None):
        if not json_data:
            json_data = json.loads(content)

        def _convert_json(parsed_json):
            new_json = None
            if isinstance(parsed_json, list):
                new_json = []
                for item in parsed_json:
                    new_json.append(_convert_json(item))
            elif isinstance(parsed_json, dict):
                new_json = {}
                for key, value in parsed_json.items():
                    key = str(key)
                    new_json[key] = _convert_json(value)
            elif isinstance(parsed_json, unicode):
                new_json = str(parsed_json)
            elif isinstance(parsed_json,
                            (int, float, long, numbers.Real, numbers.Integral,
                             str)):
                new_json = parsed_json
            return new_json

        return _convert_json(json_data)
