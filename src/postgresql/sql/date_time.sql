set client_min_messages to warning;

select util.drop_schema('date_time');
create schema date_time;

------------------------------------------------------------------
-- drop_schema
create or replace function date_time.start_of_month(dateTime timestamptz)
returns date 
as $$
declare
   som date;
begin

   som := make_date(
      cast(date_part('year', dateTime) as int),
      cast(date_part('month',dateTime) as int),
      1);      

   return som;
end;$$ language plpgsql;

create or replace function date_time.julian_days(dt date)
returns int 
as $$
declare
   jdays integer;
begin

   jdays := cast(to_char(dt, 'J') as integer);
   return jdays;
end;$$ language plpgsql;

create or replace function date_time.julian_date(days integer)
returns date 
as $$
declare
   jdate date;
begin

   jdate := to_date(cast(days as text), 'J');
   return jdate;
end;$$ language plpgsql;

