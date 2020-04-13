drop schema if exists winter_2 cascade;
create schema if not exists winter_2;

-- Supoort data:
----------------------------------------------------------------


create table if not exists winter_2.spt_public_holiday(
    holiday_date timestamp not null,
    holiday_name text,
    area text not null,
    nation text not null,

    primary key(holiday_date,area,nation)
);

