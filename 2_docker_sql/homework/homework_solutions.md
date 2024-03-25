# SOLUTIONS

# Question 3

```sql
SELECT
	COUNT(*)
FROM
	GREEN_TAXI_DATA
WHERE DATE(lpep_pickup_datetime) = '2019-09-18'
  AND DATE(lpep_dropoff_datetime) = '2019-09-18';
```
15612

# Question 4

```sql
SELECT
	DATE(lpep_pickup_datetime),
	MAX(trip_distance)
FROM
	GREEN_TAXI_DATA
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
```
2019-09-26

# Question 5

```sql
SELECT
	zones."Borough",
	SUM(taxi.total_amount)
	
FROM GREEN_TAXI_DATA AS taxi
LEFT JOIN zones_data AS zones
	   ON taxi."PULocationID" = zones."LocationID"
WHERE DATE(taxi.lpep_pickup_datetime) = '2019-09-18'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 3;
```
Brooklyn, Manhattan, Queens

# Question 6

```sql
SELECT
    dozones."Zone",
	max(taxi.tip_amount)
	
FROM GREEN_TAXI_DATA AS taxi
LEFT JOIN zones_data AS puzones
	   ON taxi."PULocationID" = puzones."LocationID"
LEFT JOIN zones_data AS dozones
	   ON taxi."DOLocationID" = dozones."LocationID"
WHERE DATE(taxi.lpep_pickup_datetime) BETWEEN '2019-09-01' AND '2019-09-30'
  AND puzones."Zone" = 'Astoria'
GROUP BY 1
ORDER BY 2 desc
LIMIT 1;
```
JFK Airport

# Question 7

```
terraform init
terraform apply
```