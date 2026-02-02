# de-zoomcap-2026-module1-homework
Module 1 Homework


## Question 1. Understanding Docker images
**Answer: 25.3**

```
docker run -it --rm \
    --rm \
    --entrypoint=bash \
    python:3.13

pip -V
```

## Question 2. Understanding Docker networking and docker-compose
**Answer: db:5432**


## Data ingestion
```
cd pipeline

docker-compose up

docker build -t green_taxi_ingest:v001 .

docker run -it --rm \
    --network=pipeline_default \
    green_taxi_ingest:v001 \
    --pg-host=pgdatabase \
    --pg-user=root \
    --pg-pass=root \
    --year=2025 \
    --month=11
```

## Question 3. Counting short trips
**Answer: 8007**

```
SELECT
        count(*)
FROM    
        public.green_taxi_data
WHERE
        lpep_pickup_datetime >= '2025-11-01'
        AND
        lpep_pickup_datetime  < '2025-12-01'
        AND
        trip_distance <= 1
;
```

## Question 4. Longest trip for each day

**Answer: 2025-11-14**


```
SELECT
        lpep_pickup_datetime::date,
        trip_distance
FROM
        public.green_taxi_data
WHERE
        trip_distance < 100
ORDER BY
        trip_distance desc
LIMIT
        1
;
```

## Question 5. Biggest pickup zone
**Answer: East Harlem North**

```
select
        zones."Zone" as zone,
        sum(data.total_amount) as total
FROM    
        public.green_taxi_data  as data  inner join
        public.taxi_zone_lookup as zones on data."PULocationID" = zones."LocationID" 
WHERE
        data.lpep_pickup_datetime::date = '2025-11-18'
GROUP BY
        zone
ORDER BY
        total desc
LIMIT
        1
;
```

## Question 6. Largest tip
**Answer: Yorkville West**

```
SELECT
        do_zones."Zone" as do_zone,
        data.tip_amount
FROM
        public.green_taxi_data  as data
        inner join
        public.taxi_zone_lookup as pu_zones on data."PULocationID" = pu_zones."LocationID"
        inner join
        public.taxi_zone_lookup as do_zones on data."DOLocationID" = do_zones."LocationID"
WHERE
        data.lpep_pickup_datetime >= '2025-11-01' and
        data.lpep_pickup_datetime  < '2025-12-01' and
        pu_zones."Zone" = 'East Harlem North'
ORDER BY
        data.tip_amount desc
LIMIT
        1
;
```

## Question 7. Terraform Workflow
**Answer: terraform init, terraform apply -auto-approve, terraform destroy**

```
terraform init
	- Reusing previous version of hashicorp/google from the dependency lock file
	- Using previously-installed hashicorp/google v7.16.0

	Terraform has been successfully initialized!


terraform apply -auto-approve
	Apply complete! Resources: 2 added, 0 changed, 0 destroyed.

terraform destroy
	Destroy complete! Resources: 2 destroyed.
```