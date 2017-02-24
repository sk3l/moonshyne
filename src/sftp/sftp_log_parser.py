#!/usr/bin/python3

import os
import os.path
import re
import sys

from sftp.sftp_account import SftpAccount
from sftp.sftp_session import SftpCommand
from sftp.sftp_session import SftpCommandTypes
from sftp.sftp_session import SftpCommandTypesMap
from sftp.sftp_session import SftpSession

from datetime import timedelta
from datetime import datetime
from datetime import time

class SftpLogParser :

    def __init__(self, fName):
        self.fname_     = fName
        self.regex_     = re.compile('^.* internal-sftp.*time=(....-..?-..? .*) user=(.*) pid=([0-9]+) (.*)$')
        self.acct_map_  = {}
        self.sess_map_  = {}

    def _parseSftpOp(self, opString):

        operation = SftpCommandTypes.Unknown
        target = ""
        opStr = ""
        for pair in SftpCommandTypesMap.items():
            if opString.startswith(pair[0]):
                operation = pair[1]
                opStr = pair[0]

        if operation != SftpCommandTypes.Unknown:
            if operation == SftpCommandTypes.StatusResponse:
                target = opString[len(opStr):]

            elif operation == SftpCommandTypes.SessionStart:
                # Grab ID address from session start command output
                ipStart = opString.find("[") + 1
                ipEnd   = opString.rfind("]")
                target  = opString[ipStart:ipEnd]

            elif opString[len(opStr)] == '"':
                start = len(opStr)+1
                for char in opString[start:]:
                    if char == "'" or char == '"':
                        break
                    target += char

        return (operation,target,)


    # Return a dictionary hashed by session key, which is MD5 of :
    #       <start_time>_<acct_name>_<pid>
    def parse(self, accountCallback, sessionCallback):
        lineCnt = 0

        accountId  = 1

        try:
            fhandle = sys.stdin
            if len(self.fname_) > 0:
                fhandle = open(self.fname_, "r")

            matchCnt= 0
            line = fhandle.readline()
            while len(line) > 0:
                try:

                    # Use regex to determine if line is of interest with respect to file
                    # processing operations.
                    # If it is, capture the desired fields
                    reMatch = self.regex_.match(line)
                    if not reMatch:
                        continue

                    matchCnt += 1
                    timestamp   = reMatch.group(1)
                    user        = reMatch.group(2)
                    pid         = reMatch.group(3)
                    operation   = reMatch.group(4)

                    logTime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

                    account = None
                    if user in self.acct_map_:
                        account = self.acct_map_[user]
                    else:
                        account = SftpAccount(accountId, user)
                        self.acct_map_[user] = account
                        accountCallback(accountId, SftpAccount.toJSON(account), 'N')
                        accountId += 1

                    # Session lookup by key (account, pid, date)

                    # Construct a key that ~should~ uniqueley identify a client session.
                    # It's possible, but unlikely, the PID could wrap in a given log
                    # period, and then be re-used by the same user.

                    session = None
                    sessionKey = None
                    sessionDaySpan = 2
                    sessionAction = ""

                    # Try to handle log rollover event by considering
                    # sessionDaySpan days back in time when probing for key
                    for i in range(sessionDaySpan):
                        logDate = logTime.date() - timedelta(i)
                        sessionKey = SftpSession.calculate_key(account.acct_id_, pid, logDate)
                        if sessionKey in self.sess_map_:
                            session = self.sess_map_[sessionKey]
                            sessionAction = "X"
                            break

                    if not session:
                        session = SftpSession(account.acct_id_, pid, logTime.date())
                        sessionKey = session.sess_id_
                        self.sess_map_[sessionKey] = session
                        sessionAction = "N"

                    cmdPair = self._parseSftpOp(operation)

                    if cmdPair[0] == SftpCommandTypes.SessionStart:
                        session.start_time_ = logTime - SftpSession.BIG_BANG
                    elif cmdPair[0] == SftpCommandTypes.SessionFinish:
                        session.end_time_ = logTime - SftpSession.BIG_BANG


                    cmdOffset = logTime - SftpSession.BIG_BANG
                    command = SftpCommand(cmdPair[0], cmdOffset, cmdPair[1])

                    session.add_command(command)

                    sessionCallback(
                        sessionKey, SftpSession.toJSON(session), sessionAction)

                except Exception as err:
                    print("Encountered error reading log line {0}: '{1}'.".format(
                        lineCnt,err))

                finally:

                    lineCnt += 1
                    if (lineCnt % 1000000) == 0:
                        print("Processed {0} lines: {1} matches so far.".format(lineCnt,matchCnt))

                    line = fhandle.readline()

            print("SFTP log line count (total) : {0}".format(lineCnt))

        except Exception as e:

            print("Encountered error at input line {0}: {1}".format(lineCnt, e))

        finally:
            fhandle.close()

