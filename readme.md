Set of utilities to work with advanced features of postgresql.

- PgCache allows using unlogged tables for caching purposes.
- Pgmq package provides a producer-consumer abstraction over pgmq extension.
- Pgcron enables scheduling jobs using pgcron extension.


## Installation
Ensure you have the required extensions installed in your PostgreSQL database:

```go
go get github.com/rizvn/pgutils
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
    "github.com/jackc/pgx/v5/pgxpool"
    
    // for pgcache
    "github.com/rizvn/pgutils/pgcache"
    
    // for pgcron
    "github.com/rizvn/pgutils/pgcron"
    
    // for pgmq consumer and producer
    "github.com/rizvn/pgutils/pgmq"
)

// Create db pool
dbPool, err := sql.Open("pgx", "postgres://user:password@localhost:5432/dbname")
if err != nil {
    panic(fmt.Sprintf("failed to create db p pool pool %v", err)
}
dbPool.SetMaxOpenConns(20)


    
//------ PgCache Example ------//
// Create instance
pgCache := &pgcache.PgCache{
    DbPool:    dbPool,
    CacheTable: "cache_table",
    TTL:       600, // 10 minutes
    StartCleanup: true
}

/* Create cache table SQL
CREATE UNLOGGED TABLE IF NOT EXISTS cache_table (
		id TEXT PRIMARY KEY,
        content BYTEA NOT NULL,
		created_on TIMESTAMPTZ NOT NULL,
		expires_on TIMESTAMPTZ NOT NULL
);
*/


// Init instance
pgCache.Init(dbPool)

// Use instance
pgCache.Put(...) 


//------ PgCron Example ------//
// Create instance
cr := &pgcron.PgCron{
    DbPool:    dbPool
}


// Init pgcron instance
cr.Init()

// Schedule a job
cr.Schedule("my_job", */5 * * * *", "INSERT INTO public.my_table (data) VALUES ('Hello, World!')")


//------ Pgmq consumer Example ------//

// create consumer
consumer := &Consumer{
    DbPool:   dbPool,
    QueueName: "test_queue", // will be created if not exists
}

// message handler runs when a message is received
consumer.MessageHandler = func(ctx context.Context, msg *PgmqMessage) {
    //... code to process message ...
}


// Init consumer
consumer.Init()
consumer.Start()


// create producer
producer := &Producer{
    DbPool: dbPool,
}

// init producer
producer.Init()

// send message
producer.Produce("test_queue", `{"content": "Hello, Test!"}`, "{}")
```

## Running Tests
The test uses testcontainers to spin up a temporary postgres instance with required extensions.
Ensure you have Docker running on your machine.
To run the tests, execute:

```bash
go test -tags=test ./...
```

