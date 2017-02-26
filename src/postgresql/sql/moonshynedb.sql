
set client_min_messages to warning;

-------------------------------------------------------------------------------
-- Create blog schema
select util.drop_schema('moonshyne_sftp');
create schema moonshyne_sftp;

-------------------------------------------------------------------------------
-- Grant usage-based privileges to skelonl agent
--grant usage on schema date_time to skelonl_agent;

--grant usage on schema blog to skelonl_agent;

--alter default privileges in schema blog grant select, insert, update, delete on tables
--to skelonl_agent;

--alter default privileges in schema blog grant execute on functions
--to skelonl_agent;

-------------------------------------------------------------------------------
-- Create moonshyne tables
select util.drop_table('moonshyne_sftp', 'servers');
select util.drop_table('moonshyne_sftp', 'accounts');
select util.drop_table('moonshyne_sftp', 'sessions');
select util.drop_table('moonshyne_sftp', 'commands');
select util.drop_table('moonshyne_sftp', 'command_types');

create table moonshyne_sftp.servers (
   server_id        serial       not null primary key,
   server_name      text         not null,
   server_ip_addr   bigint       null,
   entry_datetime   timestamp    not null
);

create table moonshyne_sftp.accounts (
   account_id        serial       not null primary key,
   account_name      text         not null,
   entry_datetime    timestamp    not null
);

create unique index idx_ak1_accounts on moonshyne_sftp.accounts (account_name);

create table moonshyne_sftp.sessions (
   session_id        serial      not null primary key,
   --server_id         serial      not null references moonshyne_sftp.servers,
   account_id        serial      not null, --references moonshyne_sftp.accounts,
   session_date      date        not null,
   session_pid       integer     not null,
   session_start     bigint      null,
   session_end       bigint      null,
   ip_address        bigint      null,
   entry_datetime    timestamp   not null
);

create table moonshyne_sftp.command_types (
   command_type      smallint    not null primary key,
   command_name      text        not null
);

create table moonshyne_sftp.commands (
   command_id        serial      not null primary key,
   session_id        serial      not null references moonshyne_sftp.sessions,
   command_seq_id    smallint    not null,
   time_offset       bigint      not null,
   command_type      smallint    not null references moonshyne_sftp.command_types,
   command_target    text        not null,
   command_source    text        not null,
   command_status    smallint    not null default 0,
   entry_datetime    timestamp   not null
);

-------------------------------------------------------------------------------
-- "SFTP" functions
create or replace function moonshyne_sftp.create_accounts(acct_json json)
returns integer
as $$
declare
   rv   integer := 0;
begin

      insert into moonshyne_sftp.accounts
         (account_id, account_name, entry_datetime)
         select
            "accountId", "accountName", current_timestamp
         from
            json_to_recordset(acct_json)
         as
         x(sessions text, "accountId" integer, "accountName" text);

      return rv;

end;$$ language plpgsql;

create or replace function moonshyne_sftp.save_accounts(acct_json json)
returns integer
as $$
declare
   rv   integer := 0;
begin

      insert into moonshyne_sftp.accounts
         (account_id, account_name, entry_datetime)
         select
            "accountId", "accountName", current_timestamp
         from
            json_to_recordset(acct_json)
         as
            x(sessions text, "accountId" integer, "accountName" text)
         where
            not exists (
               select 1 from moonshyne_sftp.accounts a where x."accountId" = a.account_id
            );

      return rv;

end;$$ language plpgsql;

create or replace function moonshyne_sftp.create_sessions(session_json json)
returns integer
as $$
declare
   rv   integer := 0;
   sid  integer := 0;
begin

      insert into moonshyne_sftp.sessions
         (account_id, session_pid, session_date, session_start,
          session_end, ip_address, entry_datetime)
      select
         "accountId", "pid", date_time.julian_date("sessionDate"),
         "startTime", "endTime", "ipAddress", current_timestamp
      from
         json_to_recordset(session_json)
      as
      x("serverId" integer,
        "accountId" integer,
        "pid" integer,
        "sessionDate" integer,
        "startTime" bigint,
        "endTime" bigint,
        "ipAddress" bigint);

      insert into moonshyne_sftp.commands
         (session_id,command_seq_id,time_offset,command_type,command_target,
          command_source,command_status,entry_datetime)
      with sess as (
         select value as sessn from json_array_elements(session_json)
      )
      select
         s2.session_id,
         cast(cmd->>'sequenceId' as smallint),
         cast(cmd->>'timeOffset' as bigint),
         cast(cmd->>'type' as int),
         cast(cmd->>'target' as text),
         cast(cmd->>'source' as text),
         cast(cmd->>'status' as int),
         current_timestamp
      from
         sess s1
            cross join
         json_array_elements(s1.sessn->'commands') cmd
            join
          moonshyne_sftp.sessions s2 on
            cast(s1.sessn->>'accountId' as int) = s2.account_id and
            cast(s1.sessn->>'pid' as int)       = s2.session_pid and
            date_time.julian_date(cast(s1.sessn->>'sessionDate' as int)) = s2.session_date;

      return rv;

end;$$ language plpgsql;

create or replace function moonshyne_sftp.update_sessions(session_json json)
returns integer
as $$
declare
   rv   integer := 0;
   sid  integer := 0;
   seq  integer := -1;
begin

   -- Update session cols
   update moonshyne_sftp.sessions
   set
      --session_start = sess.start_time,
      session_end = sess.end_time,
      ip_address  = sess.ip_address
   from
   (
      select
         "startTime" as start_time, "endTime" as end_time,
         "ipAddress" as ip_address
      from
         json_to_recordset(session_json)
      as
      x("serverId" integer,
        "accountId" integer,
        "pid" integer,
        "startTime" bigint,
        "endTime" bigint,
        "ipAddress" bigint)
   ) as sess;

   -- Insert new session commands in the update batch
   insert into moonshyne_sftp.commands
      (session_id,command_seq_id,time_offset,command_type,command_target,
          command_source,command_status,entry_datetime)
      with sess as (
         select value as sessn from json_array_elements(session_json)
      )
      select
         s2.session_id,
         cast(cmd->>'sequenceId' as smallint),
         cast(cmd->>'timeOffset' as bigint),
         cast(cmd->>'type' as int),
         cast(cmd->>'target' as text),
         cast(cmd->>'source' as text),
         cast(cmd->>'status' as int),
         current_timestamp
      from
         sess s1
            cross join
         json_array_elements(s1.sessn->'commands') cmd
            join
         moonshyne_sftp.sessions s2 on
            cast(s1.sessn->>'accountId' as int) = s2.account_id and
            cast(s1.sessn->>'pid' as int)       = s2.session_pid and
            date_time.julian_date(cast(s1.sessn->>'sessionDate' as int)) = s2.session_date
      where
         not exists
         (
            select 1 from moonshyne_sftp.commands c
            where c.session_id = s2.session_id and
            c.command_seq_id = cast(cmd->>'sequenceId' as smallint)
         );

   -- Update existing session commands in the update batch
   update moonshyne_sftp.commands
      set
         command_status = cmds.status
      from
      (
         with sess as (
            select value as sessn from json_array_elements(session_json)
         )
         select
            s2.session_id sess_id,
            cast(cmd->>'sequenceId' as smallint) cmd_seq,
            cast(cmd->>'status' as int) as status
         from
            sess s1
               cross join
            json_array_elements(s1.sessn->'commands') cmd
               join
            moonshyne_sftp.sessions s2 on
               cast(s1.sessn->>'accountId' as int) = s2.account_id and
               cast(s1.sessn->>'pid' as int)       = s2.session_pid and
               date_time.julian_date(cast(s1.sessn->>'sessionDate' as int)) = s2.session_date
      ) as cmds
      where
         session_id = sess_id and
         command_seq_id = cmd_seq;

   return rv;

end;$$ language plpgsql;

create or replace function moonshyne_sftp.save_sessions(session_json json)
returns integer
as $$
declare
   rv   integer := 0;
   sid  integer := 0;
   seq  integer := -1;
begin

   -- Create new sessions
   insert into moonshyne_sftp.sessions
      (account_id, session_pid, session_date, session_start,
          session_end, ip_address, entry_datetime)
      select
         "accountId", "pid", date_time.julian_date("sessionDate"),
         "startTime", "endTime", "ipAddress", current_timestamp
      from
         json_to_recordset(session_json)
      as
      x("serverId" integer,
        "accountId" integer,
        "pid" integer,
        "sessionDate" integer,
        "startTime" bigint,
        "endTime" bigint,
        "ipAddress" bigint)
      where not exists (
         select 1 from moonshyne_sftp.sessions s where
         s.account_id = "accountId" and
         s.session_pid = "pid" and
         s.session_date = date_time.julian_date("sessionDate") 
      );


   -- Update existing session 
   update moonshyne_sftp.sessions
   set
      --session_start = sess.start_time,
      session_end = sess.end_time,
      ip_address  = sess.ip_address
   from
   (
      select
         "startTime" as start_time, "endTime" as end_time,
         "ipAddress" as ip_address
      from
         json_to_recordset(session_json)
      as
      x("serverId" integer,
        "accountId" integer,
        "pid" integer,
        "sessionDate" integer,
        "startTime" bigint,
        "endTime" bigint,
        "ipAddress" bigint)
      where exists (
         select 1 from moonshyne_sftp.sessions s where
         s.account_id = "accountId" and
         s.session_pid = "pid" and
         s.session_date =  date_time.julian_date("sessionDate")
      )      
   ) as sess;

   -- Insert new session commands in the update batch
   insert into moonshyne_sftp.commands
      (session_id,command_seq_id,time_offset,command_type,command_target,
          command_source,command_status,entry_datetime)
      with sess as (
         select value as sessn from json_array_elements(session_json)
      )
      select
         s2.session_id,
         cast(cmd->>'sequenceId' as smallint),
         cast(cmd->>'timeOffset' as bigint),
         cast(cmd->>'type' as int),
         cast(cmd->>'target' as text),
         cast(cmd->>'source' as text),
         cast(cmd->>'status' as int),
         current_timestamp
      from
         sess s1
            cross join
         json_array_elements(s1.sessn->'commands') cmd
            join
         moonshyne_sftp.sessions s2 on
            cast(s1.sessn->>'accountId' as int) = s2.account_id and
            cast(s1.sessn->>'pid' as int)       = s2.session_pid and
            date_time.julian_date(cast(s1.sessn->>'sessionDate' as int)) = s2.session_date
      where
         not exists
         (
            select 1 from moonshyne_sftp.commands c
            where c.session_id = s2.session_id and
            c.command_seq_id = cast(cmd->>'sequenceId' as smallint)
         );

   -- Update existing session commands in the update batch
   update moonshyne_sftp.commands
      set
         command_status = cmds.status
      from
      (
         with sess as (
            select value as sessn from json_array_elements(session_json)
         )
         select
            s2.session_id sess_id,
            cast(cmd->>'sequenceId' as smallint) cmd_seq,
            cast(cmd->>'status' as int) as status
         from
            sess s1
               cross join
            json_array_elements(s1.sessn->'commands') cmd
               join
            moonshyne_sftp.sessions s2 on
               cast(s1.sessn->>'accountId' as int) = s2.account_id and
               cast(s1.sessn->>'pid' as int)       = s2.session_pid and
               date_time.julian_date(cast(s1.sessn->>'sessionDate' as int)) = s2.session_date
      ) as cmds
      where
         session_id = sess_id and
         command_seq_id = cmd_seq;

   return rv;

end;$$ language plpgsql;


--
create or replace function moonshyne_sftp.get_session_key(s_id integer)
returns text
as $$
declare
   hash text;
begin

   select
      encode(
         digest(
            cast(s.account_id as text)    || '_' ||
            cast(s.session_pid as text)  || '_' ||
            cast(date_time.julian_days(s.session_date) as text) ,
            'sha256'),
         'base64') into hash
   from
      moonshyne_sftp.sessions s
   where
      s.session_id = s_id;


   return hash;
end;$$ language plpgsql;

