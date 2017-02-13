#!/usr/bin/python3

import pycouchdb
import json
import logging

class SftpLogCdbDistiller :

    BATCH_SIZE = 1000

    def __init__(self, connStr) :

        self.conn_str_          = connStr
        self.cdb_server_        = None
        self.cdb_account_db_    = None
        self.cdb_session_db_    = None

        self.account_cache_     = {}
        self.account_batch_     = set()
        self.account_batch_cnt_ = 0

        self.session_cache_     = {}
        self.session_batch_     = set()
        self.session_batch_cnt_ = 0

    def connect(self, acctDb, sessDb):
        self.cdb_server_ = pycouchdb.Server(self.conn_str_)
        self.cdb_account_db_ = self.cdb_server_.database(acctDb)
        self.cdb_session_db_ = self.cdb_server_.database(sessDb)

    # Process a JSON document 'account' with the specified 'state', either:
    #   'N' => new
    #   'X' => existing
    def process_account(self, acctId, account, state):
        try:
            #doc = self.cdb_account_db_.save(account)

            # Preserve CouchDb metadata when updating existing accounts
            if state == 'X' and acctId in self.account_cache_:
                currAcct = self.account_cache_[acctId]
                if "_id" in currAcct:
                    account["_id"] = currAcct["_id"]
                if "_rev" in currAcct:
                    account["_rev"] = currAcct["_rev"]

            self.account_cache_[acctId] = account

            self.account_batch_.add(acctId)
            self.account_batch_cnt_ += 1

            if self.account_batch_cnt_ == SftpLogCdbDistiller.BATCH_SIZE:

                acctDocs = []
                for aid in self.account_batch_:
                    acctDocs.append(self.account_cache_[aid])

                results = self.cdb_account_db_.save_bulk(acctDocs)

                for doc in results:
                    aid = doc["accountId"]
                    self.account_cache_[aid]["_id"]  = doc["_id"]
                    self.account_cache_[aid]["_rev"] = doc["_rev"]
                
                self.account_batch_.clear()
                self.account_batch_cnt_ = 0

        except Exception as err:
            print("Error in SftpLogCdbDistiller::process_account : {0}".format(err))

    # Process a JSON document 'session' with the specified 'state', either:
    #   'N' => new
    #   'X' => existing
    def process_session(self, sessId, session, state):
        try:

            if state == 'N':
                # get account ID and update account with new session
                acctId = 0
                if "accountId" in session and isinstance(session["accountId"], int):
                    acctId = session["accountId"]

                    if acctId in self.account_cache_:
                        account = self.account_cache_[acctId]
                        account["sessions"].append(sessId)
                        self.process_account(acctId, account, "X")
            
            if sessId in self.session_cache_:
                # Preserve CouchDb metadata when updating existing accounts
                currSession = self.session_cache_[sessId]
                if "_id" in currSession:
                    session["_id"]  = currSession["_id"]
                if "_rev" in currSession:
                    session["_rev"] = currSession["_rev"]
            self.session_cache_[sessId] = session

            # Append the session update to 
            self.session_batch_.add(sessId)
            self.session_batch_cnt_ += 1

            if self.session_batch_cnt_ == SftpLogCdbDistiller.BATCH_SIZE:

                sessDocs = []
                for sid in self.session_batch_:
                    sessDocs.append(self.session_cache_[sid])

                results = self.cdb_session_db_.save_bulk(sessDocs)

                for doc in results:
                    sid = doc["sessionId"]
                    self.session_cache_[sid]["_id"] = doc["_id"]
                    self.session_cache_[sid]["_rev"] = doc["_rev"]
                
                self.session_batch_.clear()
                self.session_batch_cnt_ = 0

        except Exception as err:
            print("Error in SftpLogCdbDistiller::process_session : {0}".format(err))


    def cleanup(self):
        # Process any pending updates in the batch lists
        acctDocs = []
        for aid in self.account_batch_:
            acctDocs.append(self.account_cache_[aid])

        results = self.cdb_account_db_.save_bulk(acctDocs)
        self.cdb_account_db_.cleanup()
        self.cdb_account_db_ = None

        sessDocs = []
        for sid in self.session_batch_:
            sessDocs.append(self.session_cache_[sid])

        results = self.cdb_session_db_.save_bulk(sessDocs)
        self.cdb_session_db_.cleanup()
        self.cdb_session_db_ = None

        self.cdb_server_ = None
          
