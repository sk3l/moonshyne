#!/usr/bin/python3

import json

class SftpAccount:

    def __init__(self, acctId, acctName):
        self.acct_name_ = acctName
        self.acct_id_   = acctId
        self.sess_list_ = []

    @classmethod
    def toStr(classobj, sftpAccount):
        return SftpAccountJsonEncoder().encode(sftpAccount)

    @classmethod
    def toJSON(classobj, sftpAccount):
        jsonObj = {
            "accountName"   : sftpAccount.acct_name_,
            "accountId"     : sftpAccount.acct_id_,
            "sessions"      : sftpAccount.sess_list_}
        return jsonObj

class SftpAccountJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if not isinstance(obj, SftpAccount):
            return json.JSONEncoder.default(self, obj)

        return SftpAccount.toJSON(obj)

