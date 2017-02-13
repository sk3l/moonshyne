set client_min_messages to warning;

do $$
begin
   if exists (select 1 from pg_namespace where nspname = 'util') then
      drop schema util cascade;
   end if;
end$$;


create schema util;

--------------------------------------------------------------------------------
-- drop_database
create or replace function util.drop_database(sName varchar)
returns boolean
as $$
begin
   if exists (select 1 from pg_database where datname = sName) then
      execute 'drop database ' || sName || ';';
      return true;
   end if;
   return false;
end;$$ language plpgsql;


--------------------------------------------------------------------------------
-- drop_schema
create or replace function util.drop_schema(sName varchar)
returns boolean
as $$
begin
   if exists (select 1 from pg_namespace where nspname  = sName) then
      execute 'drop schema ' || sName || ' cascade;';
      return true;
   end if;
   return false;
end;$$ language plpgsql;

--------------------------------------------------------------------------------
-- drop_table_
create or replace function util.drop_table(sName varchar, tName varchar)
returns boolean
as $$
begin
   if exists (select 1 from pg_tables where schemaname = sName and 
   tablename = tName) then
      execute 'drop table ' || sName || '.' || tName || ';';
      return true;
   end if;
   return false;
end;$$ language plpgsql;

--------------------------------------------------------------------------------
-- drop_function_
create or replace function util.drop_function
(
   sName varchar,
   fName varchar,
   aNames varchar
)
returns boolean
as $$
begin
   if exists (select 1 from pg_proc a join pg_namespace b on a.pronamespace = 
         b.oid where b.nspname = sName and a.proname = fName) then
      execute 'drop function ' ||  sName || '.' || fName || '(' || aNames || ');';
      return true;
   end if;
   return false;
end;$$ language plpgsql;

--------------------------------------------------------------------------------
-- drop_type_
create or replace function util.drop_type(sName varchar, tName varchar)
returns boolean
as $$
begin
   if exists (select 1 from pg_type a join pg_namespace b on a.typnamespace = 
         b.oid  where b.nspname = sName and a.typname = tName) then
      execute 'drop type ' ||  sName || '.' || tName || ');';
      return true;
   end if;
   return false;
end;$$ language plpgsql;

--------------------------------------------------------------------------------
-- drop_role_
create or replace function util.drop_role(rName varchar)
returns boolean
as $$
begin
   if exists (select 1 from pg_roles where rolname = rName) then
      execute 'drop role ' ||  rName || ';';
      return true;
   end if;
   return false;
end;$$ language plpgsql;

