#!/usr/bin/python3

import argparse
import datetime
import glob
import io
import json
import logging
import os
import os.path
import sys
import time

from postgresql.pgsql_distiller import SftpLogPgSqlDistiller
from couchdb.couchdb_distiller  import SftpLogCdbDistiller 

from sftp.sftp_session          import SftpSessionJsonEncoder 
from sftp.sftp_log_parser       import SftpLogParser

log_mod = "moonshyne"

acctFile = None
sessFile = None
 
def account_callback(acct, action):
    acctFile.write(json.dumps(vars(acct)))

def session_callback(sess, action):
    sessFile.write(SftpSessionJsonEncoder().encode(sess))

exit_code = 0
try:
    argcheck = argparse.ArgumentParser(
        description="moonshyne - log distillation/enrichment .")

    argcheck.add_argument('-f',
        metavar='files', dest='files', required=True,
        help='FQN of the path where log files are located')

    argcheck.add_argument('-v',
        metavar='verbosity', dest='verbosity',
        help='Verbosity level for Python Logging framework (default=DEBUG)')

    args = argcheck.parse_args()

    logger = logging.getLogger(log_mod)

    verbosity = "DEBUG"
    if vars(args)["verbosity"]:
        verbosity = args.verbosity
    logger.setLevel(verbosity)

    formatter = logging.Formatter(
                    '%(asctime)s - %(name)s (%(levelname)s) - %(message)s')

    consHandler = logging.StreamHandler()
    consHandler.setLevel(verbosity)
    consHandler.setFormatter(formatter)
    logger.addHandler(consHandler)

    fileHandler = logging.FileHandler(
                    time.strftime("moonshyne_log_%H_%M_%m_%d_%Y.txt"))
    fileHandler.setLevel(verbosity)
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    # !!! TESTING !!!
    #logDistiller = SftpLogCdbDistiller("http://127.0.0.1:5984")
    #logDistiller.connect("sftp_accounts", "sftp_sessions")

    logDistiller = SftpLogPgSqlDistiller('host=172.17.0.2 dbname=postgres user=postgres')
    logDistiller.connect()

    #acctFile = open("accounts.txt", "w")
    #sessFile = open("sessions.txt", "w")

    for infile in glob.glob(args.files):
        parser = SftpLogParser(infile)
        parser.parse(logDistiller.process_account, logDistiller.process_session)
        #parser.parse(account_callback, session_callback)

    logDistiller.cleanup()

except Exception as e:
    logger.error("Encountered error in moonshyne::main - {0}".format(e))
    exit_code = 16 

if acctFile:
    acctFile.close()
if sessFile:
    sessFile.close()

logger.info("Exit moonshyne main.")
sys.exit(exit_code)
