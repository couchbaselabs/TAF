"""
Created on Oct 24, 2017

@author: riteshagarwal
"""

import mode

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
                _ = _.strip("`")
                full_name.append(_)
        return '.'.join(full_name)
