CREATE DATABASE IF NOT EXISTS flink_2021;
USE flink_2021;
CREATE TABLE IF NOT EXISTS user_events(
event_type int,
event_time timestamp,
user_email varchar(254),
phone_number varchar(10),
processing_date date,
file_name varchar(250));
