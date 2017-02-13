#!/usr/bin/python3

import argparse
import enum
import os
import os.path
import re
import sys

from datetime import datetime
from datetime import time

file_dict = {}

fhandle = sys.stdin

warnMessage = []
errMessage  = []
outname = "logcheck.txt"

class LogOperations(enum.Enum):
    Unknown         = 0
    SessionStart    = 1
    SessionFinish   = 2
    FileOpen        = 3
    FileClose       = 4
    ForceFileClose  = 5
    DirOpen         = 6
    DirClose        = 7
    ForceDirClose   = 8
    Mkdir           = 9
    Rmdir           = 10
    StatusResponse  = 11
    Stat            = 12
    LStat           = 13
    StatFS          = 14
    Rename          = 15
    PosixRename     = 16
    Remove          = 17
    Set             = 18

logOperationMap = {
    "session opened "   : LogOperations.SessionStart,
    "session closed "   : LogOperations.SessionFinish,
    "open "             : LogOperations.FileOpen,
    "close "            : LogOperations.FileClose,
    "forced close "     : LogOperations.ForceFileClose,
    "opendir "          : LogOperations.DirOpen,
    "closedir "         : LogOperations.DirClose,
    "forced closedir "  : LogOperations.ForceDirClose,
    "mkdir "            : LogOperations.Mkdir,
    "rmdir "            : LogOperations.Rmdir,
    "sent status "      : LogOperations.StatusResponse,
    "stat name "        : LogOperations.Stat,
    "lstat name "       : LogOperations.LStat,
    "statfs "           : LogOperations.StatFS,
    "rename old "       : LogOperations.Rename,
    "posix-rename old " : LogOperations.PosixRename,
    "remove name "      : LogOperations.Remove,
    "set "              : LogOperations.Set}

def parseOperation(opString):

    operation = LogOperations.Unknown
    target = ""
    opStr = ""
    for pair in logOperationMap.items():
        if opString.startswith(pair[0]):
            operation = pair[1]
            opStr = pair[0]

    if operation != LogOperations.Unknown:
        if operation == LogOperations.StatusResponse:
            target = opString[len(opStr):]
        elif opString[len(opStr)] == '"':
            start = len(opStr)+1
            for char in opString[start:]:
                if char == "'" or char == '"':
                    break
                target += char

    return (operation,target,)

# Check the integrity of a session's opened file and directory handles 
# (i.e. confirm they were closed), then purge the session from our map.
def checkSessionHandles(sessionKey, sessionObj, sessionMap):
    for fState in sessionObj.files_.items():  # iterate through file map
        if inWindow and not sessionObj.has_warn_ and fState[1] == 1:
            warnMessage.append(
            ("{0},time={1},file '{2}' was opened "
             "but never closed.").format(
            entry[0],sessionObj.lastOpTime_,fState[0]))
        sessionObj.has_warn_ = True

    for dState in sessionObj.directories_.items():
        if inWindow and not sessionObj.has_warn_ and dState[1] == 1:
            warnMessage.append(
            ("{0},time={1},directory '{2}' was opened "
             "but never closed.").format(
            entry[0],sessionObj.lastOpTime_,dState[0]))
            sessionObj.has_warn_ = True

    sessionMap.pop(sessionKey, 0)

class SftpSession :

    def __init__(self, op=LogOperations.Unknown,target="",time=datetime.now()):
        self.lastOp_        = op
        self.lastOpTarget_  = target
        self.lastOpTime_    = time
        self.files_         = {}    # map of files opened/closed in the session
        self.directories_   = {}    # map of dirs opened/closed in the session
        self.has_warn_      = False # only report issues with the session 1 time

# Set up a pre-compiled regex that matches and captures SFTP-specific log lines.
# This will /not/ match sshd related log lines, e.g. authen, connec events
regex = re.compile('^.* internal-sftp.*time=(....-..?-..? .*) user=(.*) pid=([0-9]+) (.*)$')

# Define a set of 'window times' for framing the log entry checks.
# The check scenarios will only be considered for entries having timestamps
# between the window start : end. This is intended to disregard entries around
# the log rollover times, which for SFTP is midnight, AFAIK
logWindowStart = time(hour=0,minute=5,second=0)
logWindowEnd = time(hour=23,minute=55,second=0)
printErrors = False

lineCnt = 1
try:

    argcheck = argparse.ArgumentParser(
        description="Scan SFTP logs for client errors and logging anomalies.")

    argcheck.add_argument('--logfile',
        metavar='logfile', required=False,
        help='FQN of SFTP log file to process')

    argcheck.add_argument('--windowStart',
        metavar='time', required=False,
        help='Start time (24hr clock format) for checking log entries.')

    argcheck.add_argument('--windowEnd',
        metavar='time', required=False,
        help='End time (24hr clock format) for checking log entries.')

    argcheck.add_argument('--printErrors',
        metavar='printErrors', required=False,
        help='Determine if client SFTP errors are printed.')

    args = argcheck.parse_args()

    if vars(args)["windowStart"]:
        logWindowStart = datetime.strptime(args.windowStart, "%H:%M:%S").time()

    if vars(args)["windowEnd"]:
        logWindowEnd = datetime.strptime(args.windowEnd, "%H:%M:%S").time()

    if vars(args)["printErrors"]:
        printErrors = bool(args.printErrors)

    name = "sftplog"
    if vars(args)["logfile"]:
        fhandle = open(args.logfile, "r")
        name = args.logfile
        outname = "{0}_logcheck.txt".format(name)

    matchCnt= 0
    line = fhandle.readline()
    while len(line) > 0:
        try:

            # Use regex to determine if line is of interest with respect to file
            # processing operations.
            # If it is, capture the desired fields
            reMatch = regex.match(line)
            if not reMatch:
                continue

            matchCnt += 1
            timestamp   = reMatch.group(1)
            user        = reMatch.group(2)
            pid         = reMatch.group(3)
            operation   = reMatch.group(4)

            # Don't check entries outside the window
            entryDateTime = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
            entryTime = entryDateTime.time()

            sessInfo= None

            # Construct a key that ~should~ uniqueley identify a client session.
            # It's possible, but unlikely, the PID could wrap in a given log
            # period, and then be re-used by the same user.

            opPair = parseOperation(operation)

            key = "user={0},pid={1}".format(user,pid)
            # Either locate an session info for the given user & PID combo,
            # or create and hash a new one.
            if key in file_dict:
                sessInfo = file_dict[key]

                if inWindow and not sessInfo.has_warn_ and opPair[0] == LogOperations.SessionStart:
                    warnMessage.append(
                    ("user={0},pid={1},time={2},detected new session "
                     "without close of previous session for same user+PID; closing old session").format(
                    user,pid,timestamp))

                    checkSessionHandles(key, sessInfo, file_dict)
                    sessInfo.has_warn_ = True

            else:
                sessInfo = SftpSession()
                file_dict[key] = sessInfo

            # Note about processing of file names in conditional statements:
            # Many Many file names carry duplicate path seperators ("/a/b//c")
            # that do nothing but create Red Herrings, fake discrepancies
            # between file name used for open vs close, so compress the dup
            # path seperators using normpath().

            inWindow = bool(entryTime > logWindowStart and entryTime < logWindowEnd)

            # If the entry time fits within the winodw, perform our checks
            if inWindow and not sessInfo.has_warn_ and sessInfo.lastOp_ == LogOperations.Unknown and opPair[0] != LogOperations.SessionStart:
                warnMessage.append(
                    ("user={0},pid={1},time={2},detected operation '{3}' "
                     "without prior session open").format(
                         user,pid,timestamp,opPair[0]))
                sessInfo.has_warn_ = True

            elif opPair[0] == LogOperations.FileOpen:
                fName = os.path.normpath(opPair[1])
                if inWindow and not sessInfo.has_warn_:
                    exists = fName in sessInfo.files_
                    status = sessInfo.files_[fName] if exists else None
                    if exists and status == 1:
                        warnMessage.append(
                            ("user={0},pid={1},time={2},detected open of file '{3}' "
                            "that had been previously open").format(
                                user,pid,timestamp,fName))
                        sessInfo.has_warn_ = True
                sessInfo.files_[fName] = 1

            elif opPair[0] == LogOperations.FileClose or opPair[0] == LogOperations.ForceFileClose:
                fName = os.path.normpath(opPair[1])
                if inWindow and not sessInfo.has_warn_:
                    exists = fName in sessInfo.files_
                    status = sessInfo.files_[fName] if exists else None
                    if not exists or status != 1:
                        warnMessage.append(
                            ("user={0},pid={1},time={2},detected close of file '{3}' "
                             "without prior open").format(
                                user,pid,timestamp,fName))
                        sessInfo.has_warn_ = True
                sessInfo.files_[fName] = 0

            elif opPair[0] == LogOperations.DirOpen:
                dName = os.path.normpath(opPair[1])
                if inWindow and not sessInfo.has_warn_ :
                    exists = dName in sessInfo.directories_
                    status = sessInfo.directories_[dName] if exists else None
                    if exists and status == 1:
                        warnMessage.append(
                            ("user={0},pid={1},time={2},detected open of directory '{3}' "
                            "that had been previously open").format(
                                 user,pid,timestamp,dName))
                        sessInfo.has_warn_ = True
                sessInfo.directories_[dName] = 1

            elif opPair[0] == LogOperations.DirClose or opPair[0] == LogOperations.ForceDirClose:
                dName = os.path.normpath(opPair[1])
                if inWindow and not sessInfo.has_warn_:
                    exists = dName in sessInfo.directories_
                    status = sessInfo.directories_[dName] if exists else None
                    if not exists or status != 1:
                        warnMessage.append(
                            ("user={0},pid={1},time={2},detected close of directory '{3}' "
                             "without prior opendir").format(
                                user,pid,timestamp,dName))
                        sessInfo.has_warn_ = True
                sessInfo.directories_[dName] = 0

            elif opPair[0] == LogOperations.StatusResponse:
                if inWindow and not sessInfo.has_warn_ and sessInfo.lastOp_ == LogOperations.Unknown or sessInfo.lastOp_ == LogOperations.StatusResponse:
                    warnMessage.append(
                        ("user={0},pid={1},time={2},detected status response '{3}' "
                         "without any prior client activity").format(
                            user,pid,timestamp,opPair[1]))
                    sessInfo.has_warn_ = True
                errMessage.append(
                    ("user={0},pid={1},time={2},"
                    "sent status message '{3}' in response to operation '{4}'.").format(
                    user,
                    pid,
                    timestamp,
                    opPair[1],
                    sessInfo.lastOp_))

                if sessInfo.lastOp_ == LogOperations.FileOpen:
                    sessInfo.files_[sessInfo.lastOpTarget_] = -1
                elif sessInfo.lastOp_ == LogOperations.DirOpen:
                    sessInfo.directories_[sessInfo.lastOpTarget_] = -1

            elif opPair[0] == LogOperations.SessionFinish:

                checkSessionHandles(key, sessInfo, file_dict)

                continue

            sessInfo.lastOp_        = opPair[0]
            sessInfo.lastOpTarget_  = os.path.normpath(opPair[1])
            sessInfo.lastOpTime_    = entryDateTime

        except Exception as err:
            print("Encountered error reading log line {0}: '{1}'.".format(
                lineCnt,err))

        finally:
            lineCnt += 1

            if (lineCnt % 1000000) == 0:
                print("Processed {0} lines: {1} matches so far.".format(lineCnt,matchCnt))

            line = fhandle.readline()

    print("Checking cleanup of files, directories, and sessions...")
    #import pdb
    #pdb.set_trace()
    for entry in file_dict.items():     # iterate through account/pid map

        sessInfo = entry[1]

        # Skip the cleanup checks for sessions that are still in progress
        # outside of the log window (i.e. good chance they'll span the log
        # rollover so close event happens in next log file)
        if sessInfo.lastOpTime_.time() > logWindowEnd:
            continue

        if not sessInfo.has_warn_:
            # Never encountered a session close entry for this session, so it
            # wasn't removed from the dictionary.
            warnMessage.append(
            ("{0},time={1},detected session missing "
             "final session close").format(
                entry[0],sessInfo.lastOpTime_))
            sessInfo.has_warn_ = True

    print("SFTP log line count (total)            :  {0}".format(lineCnt))
    print("Anomaly events (potential log issues)  :  {0}".format(len(warnMessage)))
    print("Error events (client activity issues)  :  {0}".format(len(errMessage)))

except Exception as e:

    print("Encountered error at input line {0}: {1}".format(lineCnt, e))

finally:
    fhandle.close()

    if len(warnMessage) > 0 or len(errMessage) > 0:
        with open(outname, "w") as hOut:

            hOut.write("================================================================================\n")
            hOut.write("SFTP Log Anomalies - {0} total\n".format(len(warnMessage)))
            hOut.write("================================================================================\n")
            for warning in warnMessage:
                hOut.write("{0}\n".format(warning))

            hOut.write("================================================================================\n")
            hOut.write("SFTP Client Errors - {0} total\n".format(len(errMessage)))
            hOut.write("================================================================================\n")

            if printErrors:
                for error in errMessage:
                    hOut.write("{0}\n".format(error))
            else:
                hOut.write("xxx SFTP client error details not requested. \n".format(len(errMessage)))

            hOut.flush()


