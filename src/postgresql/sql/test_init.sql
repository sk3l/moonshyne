
select moonshyne_sftp.create_account(
'[{
   "accountName": "dl785257",
   "accountId": 311,
   "sessions": ["3kDhe++EZf6FQoyNNM4CbuyGxjFcgr0XGHzgyrM0OdQ="]
}, {
   "accountName": "dl781702",
   "accountId": 111,
   "sessions": ["jV8pWVKBXbYGzK99sffQQJmQuGq+wYATlVTybXzrDkU=", "V4mXfWbKVjaLzekyuUTZ9Ud/kVM38Hcbb+j57qMiUKE="]
}]'
);

select moonshyne_sftp.create_sessions(
'
[{
   "accountId": 111,
   "ipAddress": 169090561,
   "endTime": 0,
   "pid": "24625",
   "sessionId": "V4mXfWbKVjaLzekyuUTZ9Ud/kVM38Hcbb+j57qMiUKE=",
   "commands": [{
      "target": "",
      "source": "",
      "sequenceId": 0,
      "type": 1,
      "status": 0,
      "timeOffset": 0},
      {
      "target": "/foo/bar/c.tar.gz",
      "source": "",
      "sequenceId": 1,
      "type": 13,
      "status": 0,
      "timeOffset": 100}
   ],
   "startTime": 536389921674
}, {
   "accountId": 311,
   "ipAddress": 169090562,
   "endTime": 0,
   "pid": "14725",
   "sessionId": "3kDhe++EZf6FQoyNNM4CbuyGxjFcgr0XGHzgyrM0OdQ=",
   "commands": [{
      "target": "",
      "source": "",
      "sequenceId": 0,
      "type": 1,
      "status": 0,
      "timeOffset": 0
   }],
   "startTime": 536390005538
}]
'
);

select moonshyne_sftp.update_sessions(
'
[{
   "accountId": 111,
   "ipAddress": 169090561,
   "endTime":  536389921674,
   "pid": "24625",
   "sessionId": "V4mXfWbKVjaLzekyuUTZ9Ud/kVM38Hcbb+j57qMiUKE=",
   "commands": [{
      "target": "",
      "source": "",
      "sequenceId": 0,
      "type": 1,
      "status": 0,
      "timeOffset": 0},{

      "target": "/foo/bar/c.tar.gz",
      "source": "",
      "sequenceId": 1,
      "type": 13,
      "status": 16,
      "timeOffset": 100},{

      "target": "",
      "source": "",
      "sequenceId": 2,
      "type": 2,
      "status": 0,
      "timeOffset": 200}],
   "startTime": 536389921674}, {

   "accountId": 311,
   "ipAddress": 169090562,
   "endTime": 536390005538,
   "pid": "14725",
   "sessionId": "3kDhe++EZf6FQoyNNM4CbuyGxjFcgr0XGHzgyrM0OdQ=",
   "commands": [{
      "target": "",
      "source": "",
      "sequenceId": 0,
      "type": 1,
      "status": 0,
      "timeOffset": 0}, {

      "target": "",
      "source": "",
      "sequenceId": 1,
      "type": 2,
      "status": 32,
      "timeOffset": 500
   }],
   "startTime": 536390005538
}]
'
);
