Set of utilities to work with advanced features of postgresql.

- PgCache allows using unlogged tables for caching purposes.
- Pgmq package provides a producer-consumer abstraction over pgmq extension.
- PgCron enables scheduling jobs using pgcron extension.
- PgLock provides distributed locking using advisory locks in postgres.


## Installation
Ensure you have the required extensions installed in your PostgreSQL database:

```go
go get github.com/rizvn/pgutil
```


```sql

-- if using pgmq features 
CREATE EXTENSION IF NOT EXISTS pgmq;

-- if using pgcron features
CREATE EXTENSION IF NOT EXISTS pgcron;
```

# For Running locally
Run the docker-compose under docker-compose folder to spin up a postgres instance with required extensions.
```bash
cd docker-compose
docker-compose up -d
```

See `*_test.go` files for usage examples of each package.
The standard pattern is:

```go
import (
    "context"
    "database/sql"
    "fmt"

    _ "github.com/jackc/pgx/v5/stdlib"

    // for pgcache
    "github.com/rizvn/pgutil/pgcache"

    // for pgcron
    "github.com/rizvn/pgutil/pgcron"

    // for pgmq consumer and producer
    "github.com/rizvn/pgutil/pgmq"

    // for pglock
    "github.com/rizvn/pgutil/pglock"
)

// Create db pool
// DSN example: postgres://user:password@localhost:5432/dbname?sslmode=disable
dbPool, err := sql.Open("pgx", dsn)
if err != nil {
    panic(fmt.Sprintf("failed to create db pool: %v", err))
}
dbPool.SetMaxOpenConns(20)

//------ PgCache Example ------//
pgCache := pgcache.NewPgCache(dbPool, "cache_table", pgcache.WithTTL(600))

/* Create cache table SQL
CREATE UNLOGGED TABLE IF NOT EXISTS cache_table (
    id TEXT PRIMARY KEY,
    content BYTEA NOT NULL,
    created_on TIMESTAMPTZ NOT NULL,
    expires_on TIMESTAMPTZ NOT NULL
);
*/

if err := pgCache.CreateCacheTable(); err != nil {
    panic(err)
}

if err := pgCache.Put("cache-key", []byte("value")); err != nil {
    panic(err)
}

//------ PgCron Example ------//
cron := pgcron.NewPgCron(dbPool)

if err := cron.Schedule("my_job", "*/5 * * * *", "INSERT INTO public.my_table (data) VALUES ('Hello, World!')"); err != nil {
    panic(err)
}

//------ Pgmq Consumer/Producer Example ------//
consumer, err := pgmq.NewConsumer(
    dbPool,
    "test_queue", // will be created if not exists
    func(ctx context.Context, msg *pgmq.PgmqMessage) {
        // ... process message ...
    },
)
if err != nil {
    panic(err)
}

consumer.Start()
defer consumer.ShutdownWithWait()

producer := pgmq.NewProducer(dbPool)
if err := producer.Produce("test_queue", `{"content":"Hello, Test!"}`, "{}"); err != nil {
    panic(err)
}

//------ PgLock for distributed locking ------//
pgLockHelper := pglock.NewPgLockHelper(dbPool)

lock, err := pgLockHelper.Lock("test-lock")
if err != nil {
    panic(err)
}
defer lock.Unlock()
```

## Running Tests
The test uses testcontainers to spin up a temporary postgres instance with required extensions.
Ensure you have Docker running on your machine.
To run the tests, execute:

```bash
go test ./...
```
