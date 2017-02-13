#!/usr/bin/python3

import base64
import datetime
import enum
import hashlib
import ipaddress
import json

# Set of possible client commands in an SFTP session
class SftpCommandTypes(enum.Enum):
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

# String 
SftpCommandTypesMap = {
    "session opened "   : SftpCommandTypes.SessionStart,
    "session closed "   : SftpCommandTypes.SessionFinish,
    "open "             : SftpCommandTypes.FileOpen,
    "close "            : SftpCommandTypes.FileClose,
    "forced close "     : SftpCommandTypes.ForceFileClose,
    "opendir "          : SftpCommandTypes.DirOpen,
    "closedir "         : SftpCommandTypes.DirClose,
    "forced closedir "  : SftpCommandTypes.ForceDirClose,
    "mkdir "            : SftpCommandTypes.Mkdir,
    "rmdir "            : SftpCommandTypes.Rmdir,
    "sent status "      : SftpCommandTypes.StatusResponse,
    "stat name "        : SftpCommandTypes.Stat,
    "lstat name "       : SftpCommandTypes.LStat,
    "statfs "           : SftpCommandTypes.StatFS,
    "rename old "       : SftpCommandTypes.Rename,
    "posix-rename old " : SftpCommandTypes.PosixRename,
    "remove name "      : SftpCommandTypes.Remove,
    "set "              : SftpCommandTypes.Set}

# Set of possible client commands in an SFTP session
class SftpStatusTypes(enum.Enum):
    Success         = 0
    Failure         = 1
    NoSuchFile      = 2
    Unsupported     = 3
    PermissionDenied= 4

class SftpCommand:

    def __init__(self, cmdType, timeOffset, target="", source=""):
        self.cmd_type_      = cmdType
        self.time_offset_   = timeOffset
        self.target_        = target
        self.source_        = source
        self.status_        = SftpStatusTypes.Success 

# Encapsulate the data and context of a client's SFTP session, as described in
# an SFTP log file.
class SftpSession :

    # The beginning of our time offset
    BIG_BANG = datetime.datetime(2000,1,1,0,0,0) 

    def __init__(self, acctId, startDateTime, pid):
        # These fields uniquely identify an SFTP session
        #   => start time (*as offset from 1/1/2000*)
        #   => numeric account ID
        #   => numeric Linux process ID
        self.start_time_  = startDateTime - SftpSession.BIG_BANG 
        self.acct_id_     = acctId 
        self.pid_         = pid 
      
        self.sess_id_     = SftpSession.calculate_key(acctId, startDateTime, pid)
       
        self.start_dt_    = startDateTime
        self.end_time_    = 0 
        self.ip_addr_     = 0

        self.commands_    = []

    @classmethod
    def calculate_key(classobj, acctId, startTime, pid):
        datediff = startTime - SftpSession.BIG_BANG
        
        keyhash = hashlib.sha256()
        key = "{0}_{1}_{2}".format(int(datediff.total_seconds()*1000),acctId,pid)
        keyhash.update(key.encode('utf-8'))
        return str(base64.b64encode(keyhash.digest()))

    def session_start_as_milliseconds(self):
        if self.start_time_:
            return int(self.start_time_.total_seconds() * 1000)
        else:
            return 0

    def session_end_as_milliseconds(self):
        if self.end_time_:
            return int(self.end_time_.total_seconds() * 1000)
        else:
            return 0

    def add_command(self, sftpCommand):

        if sftpCommand.cmd_type_ == SftpCommandTypes.StatusResponse:
            cmdCnt = len(self.commands_)
            if cmdCnt > 0:
                lastCommand = self.commands_[cmdCnt-1]
                lastCommand.status_ = sftpCommand.status_

        else:
            if sftpCommand.cmd_type_ == SftpCommandTypes.SessionStart:
                self.ip_addr_ = ipaddress.IPv4Address(sftpCommand.target_)

            self.commands_.append(sftpCommand)

    @classmethod
    def toStr(classobj, sftpSession):
        return SftpSessionJsonEncoder().encode(sftpSession)

    @classmethod
    def toJSON(classobj, sftpSession):
        jsonObj = {
            "sessionId" : sftpSession.sess_id_,
            "accountId" : sftpSession.acct_id_,
            "pid"       : sftpSession.pid_,
            "startTime" : sftpSession.session_start_as_milliseconds(),
            "endTime"   : sftpSession.session_end_as_milliseconds(),
            "ipAddress" : int(sftpSession.ip_addr_)}
        
        cmdList = []
        for i in range(len(sftpSession.commands_)):
            cmd = sftpSession.commands_[i]
            cmdList.append({
                "sequenceId" : i,
                "type"       : cmd.cmd_type_.value,
                "timeOffset" : int(cmd.time_offset_.total_seconds() * 1000),  
                "target"     : cmd.target_,
                "source"     : cmd.source_,
                "status"     : cmd.status_.value})

        jsonObj["commands"] = cmdList
        return jsonObj

class SftpSessionJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if not isinstance(obj, SftpSession):
            return json.JSONEncoder.default(self, obj)

        return SftpSession.toJSON(obj)

