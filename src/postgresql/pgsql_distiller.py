#!/usr/bin/python3

import json
import logging
import psycopg2

class SftpLogPgSqlDistiller :

    BATCH_SIZE      = 1000
    ACCT_BATCH_SIZE = 50

    def __init__(self, connStr) :

        self.conn_str_          = connStr
        self.pgdb_server_       = None

        self.account_cache_     = {}
        self.account_batch_     = set()
        self.account_batch_cnt_ = 0

        self.session_cache_     = {}
        self.session_batch_     = set()
        self.session_batch_cnt_ = 0

    def connect(self):
        self.pgdb_server_ =  psycopg2.connect(self.conn_str_)

    # Process a JSON document 'account' with the specified 'state', either:
    #   'N' => new
    #   'X' => existing
    def process_account(self, acctId, account, state):
        try:

            self.account_cache_[acctId] = account

            # Only handling new account creation; don't need to update
            # account to session relationships.
            if state == 'N':
                self.account_batch_.add(acctId)
            self.account_batch_cnt_ += 1

            if self.account_batch_cnt_ == SftpLogPgSqlDistiller.ACCT_BATCH_SIZE:

                pgsql_cmd = None
                try:
                    acctDocs = []
                    for aid in self.account_batch_:
                        acctDocs.append(self.account_cache_[aid])

                    pgsql_cmd = self.pgdb_server_.cursor()

                    acctStr = json.dumps(acctDocs)
                    pgsql_cmd.execute("select moonshyne_sftp.create_accounts(cast(%s as json));",
                        (acctStr,))

                    self.account_batch_.clear()
                    self.account_batch_cnt_ = 0

                    self.pgdb_server_.commit()
                finally:
                    if pgsql_cmd:
                        pgsql_cmd.close()
                        pgsql_cmd = None

        except Exception as err:
            print("Error in SftpLogPgSqlDistiller::process_account : {0}".format(err))

    # Process a JSON document 'session' with the specified 'state', either:
    #   'N' => new
    #   'X' => existing
    def process_session(self, sessId, session, state):
        try:

            if sessId in self.session_cache_:
                oldSess = self.session_cache_[sessId]
                if "wasSaved" in oldSess:
                    session["wasSaved"] = True

            self.session_cache_[sessId] = session

            # Append the session update to
            self.session_batch_.add(sessId)
            self.session_batch_cnt_ += 1

            if self.session_batch_cnt_ == SftpLogPgSqlDistiller.BATCH_SIZE:
                pgsql_cmd = None
                try:
                    newSessDocs = []
                    updSessDocs = []

                    for sid in self.session_batch_:

                        sess = self.session_cache_[sid]
                        if not "wasSaved" in sess:
                            newSessDocs.append(sess)
                        else:
                            updSessDocs.append(sess)

                    pgsql_cmd = self.pgdb_server_.cursor()

                    sessStr = ""

                    if len(newSessDocs) > 0:
                        sessStr = json.dumps(newSessDocs)
                        pgsql_cmd.execute("select moonshyne_sftp.create_sessions(cast(%s as json))",
                            (sessStr,))
                        for newSess in newSessDocs:
                            self.session_cache_[newSess["sessionId"]]["wasSaved"] = True

                    if len(updSessDocs) > 0:
                        sessStr = json.dumps(updSessDocs)
                        pgsql_cmd.execute("select moonshyne_sftp.update_sessions(cast(%s as json));",
                            (sessStr,))
                        # DEBUG #
                        #with open("look.out", 'w') as fo:
                        #    fo.write(sessStr)
                        #    fo.flush()

                    self.session_batch_.clear()
                    self.session_batch_cnt_ = 0

                    self.pgdb_server_.commit()

                finally:
                    if pgsql_cmd:
                        pgsql_cmd.close()
                        pgsql_cmd = None

        except Exception as err:
            print("Error in SftpLogPgSqlDistiller::process_session : {0}".format(err))


    def cleanup(self):
        # Process any pending updates in the batch lists
        acctDocs = []
        for aid in self.account_batch_:
            acctDocs.append(self.account_cache_[aid])

        pgsql_cmd = None
        try:
            pgsql_cmd = self.pgdb_server_.cursor()

            acctStr = json.dumps(acctDocs)
            pgsql_cmd.execute("select moonshyne_sftp.create_accounts(cast(%s as json));",
                (acctStr,))

            newSessDocs = []
            updSessDocs = []

            for sid in self.session_batch_:

                sess = self.session_cache_[sid]
                if not "wasSaved":
                    newSessDocs.append(sess)
                else:
                    updSessDocs.append(sess)

            cursor = self.pgdb_server_.cursor()

            sessStr = ""
            if len(newSessDocs) > 0:
                sessStr = json.dumps(newSessDocs)
                cursor.execute("select moonshyne_sftp.create_sessions(%s)",
                    (sessStr,))
                for newSess in newSessDocs:
                    self.session_cache_[newSess["sessionId"]]["wasSaved"] = True

            if len(updSessDocs) > 0:
                sessStr = json.dumps(updSessDocs)
                cursor.execute("select moonshyne_sftp.update_sessions(%s);",
                    (sessStr,))

            self.pgdb_server_.commit()

        finally:
            if pgsql_cmd:
                pgsql_cmd.close()
                pgsql_cmd = None

        self.pgdb_server_.close()
        self.pgdb_server_ = None
