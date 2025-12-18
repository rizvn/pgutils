Set of utilities to work with advanced features of postgresql.

- PgCache allows using unlogged tables for caching purposes.
- Pgmq package provides a producer-consumer abstraction over pgmq extension.
- Pgcron enables scheduling jobs using pgcron extension.


## Installation
Ensure you have the required extensions installed in your PostgreSQL database:

```sql

-- if using pgmq features 
CREATE EXTENSION IF NOT EXISTS pgmq;

-- if using pgcron features
CREATE EXTENSION IF NOT EXISTS pgcron;
```

# For Testing locally
Run the docker-compose under docker-compose folder to spin up a postgres instance with required extensions.
```bash
cd docker-compose
docker-compose up -d
```

See `*_test.go` files for usage examples of each package.

