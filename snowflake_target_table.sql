use database puran_db
use schema puran_sc

create or replace table flight_data(
Indx integer,
airline string,
flight string, 
source_city string,
departure_time string,
stops string,
arrival_time string,
destination_city string,
class_type string,
duration float,
days_left integer,
price integer
);
