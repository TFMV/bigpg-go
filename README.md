# bigpg-go

## Overview

bigpg-go is a prototype for a high-performance, Arrow-native data exchange framework between PostgreSQL and BigQuery, built entirely in Go.

It leverages:

- Apache Arrow Database Connectivity (ADBC) for efficient PostgreSQL querying in a columnar format.
- BigQuery Storage Write API
- Protocol Buffers (protobuf) to convert Arrow records into the format required by BigQuery’s ingestion pipeline.

This approach eliminates unnecessary serialization overhead, reducing latency and maximizing throughput.

## Demo

You'll first need to create a table in postgres, insert some data, then create a bigquery table with the same schema.

In Postgres:

```sql
CREATE TABLE records (
    id BIGINT,
    name TEXT,
    score DOUBLE PRECISION
);

INSERT INTO records (id, name, score) VALUES
    (1, 'Alice', 95.5),
    (2, 'Bob', 89.2),
    (3, 'Charlie', 76.8),
    (4, 'Diana', 88.0),
    (5, NULL, 92.3),
    (6, 'Eve', NULL),
    (7, 'Frank', 81.4); 
```

In BigQuery:

```sql
CREATE TABLE `bigpg-go.test.records` (
    id INT64,
    name STRING,
    score FLOAT64
);
```

Then, you can run the program:

```bash
go run cmd/main.go
```

You should see output like the following:

```bash
Writing record with 7 rows and 3 columns
Sending AppendRowsRequest to stream: projects/bigpg-go/datasets/test/tables/records/streams/Cic2YmQxOTg5Ny0wMDAwLTIwNTYtODEwMC0xNDIyM2JjNWUzNDY6czU
Response received: append_result:{offset:{}} write_stream:"projects/bigpg-go/datasets/test/tables/records/streams/Cic2YmQxOTg5Ny0wMDAwLTIwNTYtODEwMC0xNDIyM2JjNWUzNDY6czU"
Successfully wrote record to BigQuery
Data migration completed successfully.
```

You can then query the data in bigquery:

```sql
SELECT * FROM `bigpg-go.test.records`;
```

You should see the following output:

| id | name | score |
|----|------|-------|
| 1  | Alice | 95.5  |
| 2  | Bob   | 89.2  |
| 3  | Charlie | 76.8  |
| 4  | Diana | 88.0  |
| 5  | NULL  | 92.3  |
| 6  | Eve   | NULL  |
| 7  | Frank | 81.4  |
