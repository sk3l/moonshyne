
delete from moonshyne_sftp.command_types;

-- Set up test servers
delete from moonshyne_sftp.servers;
insert into moonshyne_sftp.servers values (1, 'tester1', 0, current_timestamp);

insert into moonshyne_sftp.command_types (command_type, command_name) values
(0, 'Unknown');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(1, 'SessionStart');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(2, 'SessionEnd');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(3, 'FileOpen');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(4, 'FileClose');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(5, 'ForceFileClose');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(6, 'DirOpen');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(7, 'DirClose');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(8, 'ForceFileClose');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(9, 'Mkdir');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(10, 'Rmdir');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(11, 'StatusResponse');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(12, 'Stat');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(13, 'LStat');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(14, 'StatFS');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(15, 'Rename');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(16, 'PosixRename');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(17, 'Remove');
insert into moonshyne_sftp.command_types (command_type, command_name) values
(18, 'Set');

