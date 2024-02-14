CREATE EXTERNAL TABLE refined-outlet-411413.ny_taxi.green_taxi_data_2022
OPTIONS (
  format = 'PARQUET', 
  uris = [
          'gs://nyc-taxi-data-alex/green_tripdata_2022-*.parquet'
          ]
);

CREATE OR REPLACE TABLE refined-outlet-411413.ny_taxi.green_taxi_data_2022_non_partitioned AS
SELECT * FROM refined-outlet-411413.ny_taxi.green_taxi_data_2022;

SELECT DISTINCT(PULocationID) FROM refined-outlet-411413.ny_taxi.green_taxi_data_2022;

SELECT DISTINCT(PULocationID) FROM refined-outlet-411413.ny_taxi.green_taxi_data_2022_non_partitioned;

SELECT COUNT(*) FROM refined-outlet-411413.ny_taxi.green_taxi_data_2022_non_partitioned
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE refined-outlet-411413.ny_taxi.green_taxi_data_2022_partitioned_clustered
PARTITION BY 
  DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM refined-outlet-411413.ny_taxi.green_taxi_data_2022;

SELECT DISTINCT(PULocationID) FROM refined-outlet-411413.ny_taxi.green_taxi_data_2022_non_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

SELECT DISTINCT(PULocationID) FROM refined-outlet-411413.ny_taxi.green_taxi_data_2022_partitioned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

