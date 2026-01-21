# Module 1 Homework: Docker & SQL

**Note: My answers are in bold**

## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

- **25.3** I arrived at this answer by running `pipeline $ docker run --rm python:3.13 pip --version`
- 24.3.1
- 24.2.1
- 23.3.1


## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- **db:5432** 

**When containers are in the same Docker Compose network, they can communicate using service names as hostnames. `db` is the name of the service in the first half of docker-compose.yaml. `5433` is the port on the host machine (localhost) `5432` indicates the port inside the container network.**

If multiple answers are correct, select any 


## Question 3. Counting short trips

For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

- 7,853
- **8,007**
- 8,254
- 8,421

**SQL used:**
```sql
select count(1) from 
yellow_taxi_trips_2025_11
WHERE lpep_pickup_datetime < '2025-12-01'
AND trip_distance <= 1.00
```

## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

Use the pick up time for your calculations.

- **2025-11-14**
- 2025-11-20
- 2025-11-23
- 2025-11-25

**SQL Used:**
```sql
select lpep_pickup_datetime, MAX(trip_distance)
from yellow_taxi_trips_2025_11
WHERE lpep_pickup_datetime < '2025-12-01'
AND trip_distance <= 100
GROUP BY lpep_pickup_datetime
ORDER BY MAX(trip_distance) desc
LIMIT 1
```

## Question 5. Biggest pickup zone

Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

- **East Harlem North**
- East Harlem South
- Morningside Heights
- Forest Hills

**SQL Used:**
```sql
select tz."LocationID", tz."Zone", SUM(t."total_amount") from
taxi_zones tz
JOIN yellow_taxi_trips_2025_11 t ON t."PULocationID" = tz."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-18' AND t.lpep_pickup_datetime < '2025-11-19'
GROUP BY (tz."LocationID", tz."Zone")
ORDER BY SUM(t."total_amount") desc
LIMIT 1
```

## Question 6. Largest tip

For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

Note: it's `tip` , not `trip`. We need the name of the zone, not the ID.

- JFK Airport
- **Yorkville West**
- East Harlem North
- LaGuardia Airport

**SQL Used:**
```sql
select tz2."LocationID", tz2."Zone", MAX(t."tip_amount") from
yellow_taxi_trips_2025_11 t
JOIN taxi_zones tz ON t."PULocationID" = tz."LocationID"
JOIN taxi_zones tz2 ON t."DOLocationID" = tz2."LocationID"
WHERE tz."LocationID" = 74
GROUP BY (tz2."LocationID", tz2."Zone")
ORDER BY MAX(t."tip_amount") desc
LIMIT 1
```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform.
Copy the files from the course repo
[here](../../../01-docker-terraform/terraform/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, respectively, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm


