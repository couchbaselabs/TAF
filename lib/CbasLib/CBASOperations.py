'''
Created on Oct 24, 2017

@author: riteshagarwal
'''

import mode

if mode.java:
    from CbasLib.CBASOperations_JavaSDK import CBASHelper as cbaslib
elif mode.cli:
    from CbasLib.CBASOperations_CLI import CBASHelper as cbaslib
else:
    from CbasLib.CBASOperations_Rest import CBASHelper as cbaslib
    
class CBASHelper(cbaslib):
    pass