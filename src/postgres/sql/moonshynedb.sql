
set client_min_messages to warning;

-------------------------------------------------------------------------------
-- Create blog schema
select util.drop_schema('moonshyne');
create schema moonshyne;

-------------------------------------------------------------------------------
-- Grant usage-based privileges to skelonl agent
grant usage on schema date_time to skelonl_agent;

grant usage on schema blog to skelonl_agent;

alter default privileges in schema blog grant select, insert, update, delete on tables 
to skelonl_agent;

alter default privileges in schema blog grant execute on functions 
to skelonl_agent;

-------------------------------------------------------------------------------
-- Create blog tables
select util.drop_table('blog', 'entry_categories');
select util.drop_table('blog', 'entry_tags');
select util.drop_table('blog', 'entry');

create table blog.entry (
   entry_id          serial       not null primary key,
   entry_title       text         not null,
   entry_subtitle    text         null,
   entry_datetime    timestamp    not null,
   entry_text        text         not null,
   entry_handle         text         null
);

create unique index idx_ak1_entry on blog.entry (entry_title);

select util.drop_table('blog', 'category');
create table blog.category (
   category_id       serial       not null primary key,
   category_name     text         not null
);

create table blog.entry_categories (
   entry_id          serial      not null references blog.entry,
   category_id       serial      not null references blog.category,

   primary key(entry_id, category_id)
);

select util.drop_table('blog', 'tags');
create table blog.tags (
   tag_id            serial      not null primary key,
   tag_name          text        not null
);

create table blog.entry_tags (
   entry_id          serial      not null references blog.entry,
   tag_id            serial      not null references blog.tags,

   primary key(entry_id,tag_id) 
);

-------------------------------------------------------------------------------
-- "Blog" functions 
create or replace function blog.select_blog_index(months integer)
returns table (idx integer, start_ts timestamp, end_ts timestamp, bcnt bigint, title text)
as $$
declare
   now timestamp := current_timestamp;
   sql varchar := '';
   i   integer := 0;
begin

   return query
   select 
      brng.idx, brng.start_ts, brng.end_ts, count(e.entry_id) as bcnt, brng.title
      from
      (
         -- Build a reference list of month intervals between "now" and the start of
         -- the index period
         select 
            idx.idx, date_time.start_of_month(now) - make_interval(0,idx.idx) as start_ts,
            case 
               when idx.idx = 0 then now
            else 
               date_time.start_of_month(now) - make_interval(0,idx.idx - 1)
            end  as end_ts,
            case
               when idx.idx = 0 then 'Current'
            else
               to_char(date_time.start_of_month(now) - make_interval(0,idx.idx), 'Month YYYY')
            end as title
         from 
            generate_series(0,months) as idx

         union
         -- Append a special "all" interval to collect all blog posts.
         select
            -1, date_time.start_of_month(now) - make_interval(0,months), now as ent_ts,
            'All' as title
      ) brng
            join
         blog.entry e on e.entry_datetime >= brng.start_ts and e.entry_datetime < brng.end_ts
      group by
         brng.idx, brng.start_ts, brng.end_ts, brng.title
      order by
         brng.idx; 

end;$$ language plpgsql;

create or replace function blog.select_blog_categories()
returns table (category_id integer, category_name text, cnt bigint)
as $$
declare
begin

   return query
   select
      c.category_id, c.category_name, count(e.entry_id) as pcnt 
   from
      blog.category c
         join blog.entry_categories ec on c.category_id = ec.category_id
         join blog.entry e on ec.entry_id = e.entry_id
   group by
      c.category_id, c.category_name;

end;$$ language plpgsql;

--
-- Given a string representing a blog title, transform it into a blog handle.
-- Effectively, the transformation is to scan the text, replace space with 
-- hyphen, and then drop any other charcter that is a member of URL 
-- encoding reserved character set.
create or replace function blog.make_blog_handle(title text)
returns text 
as $$
declare
begin

   title = regexp_replace(title, ' ', '-', 'g');
   return regexp_replace(title, '[^0-9a-zA-Z_~.-]', '', 'g');

end;$$ language plpgsql;

--
--
select util.drop_type('blog', 'period_entry');
create type blog.period_entry
as (
   start_ts       timestamp,
   end_ts         timestamp,
   entry_id       integer,
   entry_title    text,
   entry_subtitle text,
   entry_datetime timestamp,
   entry_snippet  text,
   entry_handle      text
);

-- 
--
create or replace function blog.select_blog_period
(
   start_ts timestamp,
   end_ts   timestamp,
   pagesize integer default 5,
   pagenum  integer default 0,
   out   recordcnt   integer,
   out   resultset   blog.period_entry[]
)
as $$
declare
   r blog.period_entry;
begin

   select 
      count(entry_id) into recordcnt 
   from 
      blog.entry
   where 
      entry_datetime >= start_ts and entry_datetime < end_ts; 

   select 
      array_agg(rset)
   from
   (
      select
         start_ts, end_ts,
         entry_id, entry_title, entry_subtitle, entry_datetime,
         substring(entry_text from 0 for 200) as snippet, entry_handle
      from
         blog.entry
      where 
         entry_datetime >= start_ts and entry_datetime < end_ts
      order by
         entry_datetime desc
      limit pagesize offset pagenum * pagesize
   ) rset
   into resultset;

end;$$ language plpgsql;

-- 
--
create or replace function blog.select_blog_entry(eid integer)
returns table 
(entry_id integer, title text, subtitle text, entry_ts timestamp, entry_text text, entry_handle text, tags text)
as $$
declare
begin

   return query
   select
      e.entry_id, e.entry_title, e.entry_subtitle,
      e.entry_datetime, e.entry_text, e.entry_handle,
      string_agg(t.tag_name, ';') 
   from
      blog.entry e
         join blog.entry_tags et on e.entry_id = et.entry_id
         join blog.tags t on et.tag_id = t.tag_id
   where
      e.entry_id = eid
   group by
      e.entry_id, e.entry_title, e.entry_subtitle,
      e.entry_datetime, e.entry_text, e.entry_handle;

end;$$ language plpgsql;

-- 
--
create or replace function blog.select_blog_entry_by_handle(handle text)
returns table (entry_id integer, title text, subtitle text, entry_ts timestamp, entry_text text, entry_handle text, tags text)
as $$
declare
begin

   return query
   select
      e.entry_id, e.entry_title, e.entry_subtitle,
      e.entry_datetime, e.entry_text, e.entry_handle,
      string_agg(t.tag_name, ';') 
   from
      blog.entry e
         join blog.entry_tags et on e.entry_id = et.entry_id
         join blog.tags t on et.tag_id = t.tag_id
   where
      e.entry_handle = handle 
   group by
      e.entry_id, e.entry_title, e.entry_subtitle,
      e.entry_datetime, e.entry_text, e.entry_handle;

end;$$ language plpgsql;



--
--
create or replace view blog.v_category_entries
as
   select
      c.category_id, c.category_name,
      e.entry_id, e.entry_title, e.entry_subtitle, e.entry_datetime, 
      e.entry_text, e.entry_handle
   from
      blog.category c
         join blog.entry_categories ec on ec.category_id = c.category_id
         join blog.entry e on ec.entry_id = e.entry_id;

--
--
select util.drop_type('blog', 'category_entry');
create type blog.category_entry
as (
   category_id    integer,
   category_name  text,
   entry_id       integer,
   entry_title    text,
   entry_subtitle text,
   entry_datetime timestamp,
   entry_snippet  text,
   entry_handle      text
);
-- 
--
create or replace function blog.select_blog_entry_by_category
(
         cid         integer,
         pagesize    integer default 5,
         pagenum     integer default 0,
   out   recordcnt   integer,
   out   resultset   blog.category_entry[]
)
as $$
declare
   vr blog.v_category_entries;
   r blog.category_entry;
begin

   select 
      count(entry_id) into recordcnt 
   from 
      blog.v_category_entries
   where 
      category_id = cid;
 
   select 
      array_agg(rset)
   from
   (
      select
         v.category_id, v.category_name, 
         v.entry_id, v.entry_title, v.entry_subtitle, v.entry_datetime,
         substring(v.entry_text from 0 for 200) as snippet, v.entry_handle
      from
         blog.v_category_entries v
      where 
         category_id = cid
      order by
         v.entry_datetime desc
      limit pagesize offset pagenum * pagesize
   ) rset
   into resultset;

end;$$ language plpgsql;

--
--
select util.drop_type('blog', 'tag_entry');
create type blog.tag_entry
as (
   tag_id         integer,
   tag_name       text,
   entry_id       integer,
   entry_title    text,
   entry_subtitle text,
   entry_datetime timestamp,
   entry_snippet  text,
   entry_handle   text 
);

--
--
create or replace view blog.v_tag_entries
as
   select
      t.tag_id, t.tag_name,
      e.entry_id, e.entry_title, e.entry_subtitle, e.entry_datetime, 
      e.entry_text, e.entry_handle
   from
      blog.tags t
         join blog.entry_tags et on t.tag_id = et.tag_id
         join blog.entry e on et.entry_id = e.entry_id;

-- 
--
create or replace function blog.select_blog_entry_by_tag
(
         tag        text, 
         pagesize   integer default 5,
         pagenum    integer default 0,
   out   recordcnt  integer,
   out   resultset  blog.tag_entry[]
)
as $$
declare
begin
   
   select 
      count(entry_id) into recordcnt 
   from 
      blog.v_tag_entries
   where 
      tag_name like '%' || tag || '%';
  
   select
      array_agg(rset)
   from
   (
      select
         v.tag_id, v.tag_name,
         v.entry_id, v.entry_title, v.entry_subtitle, v.entry_datetime, 
         substring(v.entry_text from 0 for 200) as snippet, v.entry_handle
      from
         blog.v_tag_entries v
      where 
         tag_name like '%' || tag || '%'
      order by
         v.entry_datetime desc
      limit pagesize offset pagenum * pagesize 
   ) rset
   into resultset;   

end;$$ language plpgsql;
