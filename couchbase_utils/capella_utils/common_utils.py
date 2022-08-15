'''
Created on Aug 10, 2022

@author: ritesh.agarwal
'''


class Pod:
    def __init__(self, url_public, TOKEN_FOR_INTERNAL_SUPPORT=None):
        self.url_public = url_public
        self.TOKEN = TOKEN_FOR_INTERNAL_SUPPORT


class Tenant:
    def __init__(self, id, user, pwd,
                 secret=None, access=None):
        self.id = id
        self.user = user
        self.pwd = pwd
        self.api_secret_key = secret
        self.api_access_key = access
        self.project_id = None
        self.clusters = dict()
