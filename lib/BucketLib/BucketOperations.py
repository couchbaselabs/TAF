'''
Created on Oct 24, 2017

@author: riteshagarwal
'''
import mode

if mode.java:
    from BucketOperations_JavaSDK import BucketHelper as bucketlib
elif mode.cli:
    from BucketOperations_CLI import BucketHelper as bucketlib
else:
    from BucketOperations_Rest import BucketHelper as bucketlib


class BucketHelper(bucketlib):
    pass
