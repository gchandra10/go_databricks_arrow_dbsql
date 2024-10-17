# Query Databricks using GO - Arrow

This is generic **Go application** using **Arrow Array** to query Databricks SQL. Update the query to suit your needs.

```
func getData(){
    query := `SELECT * FROM samples.nyctaxi.trips`
}
```

## Setup

- rename .env_template to .env
- Update .env file

```
DATABRICKS_ACCESS_TOKEN=
DATABRICKS_HOST=
DATABRICKS_HTTP_PATH=/sql/....
```

## Preparing environment

- go mod vendor
- go mod tidy
- go mod verify
- go run .