# PidginMQ

![PidginMQ](/images/pidginmq.png)

PidginMQ is a job queuing and task scheduling library for Node.js and JavaScript, built on PostgreSQL.

The library is heavily inspired by [River](https://riverqueue.com/)

[![PostgreSQL Version](https://img.shields.io/badge/PostgreSQL-11+-blue.svg?maxAge=2592000)](http://www.postgresql.org)
[![npm version](https://badge.fury.io/js/pidginmq.svg)](https://badge.fury.io/js/pidginmq)

[Documentation](https://pidginmq.com)

## Installation

YARN:

```bash
yarn add pidginmq
```

NPM:

```bash
npm install pidginmq -S
```

## Requirements

- Node 18 or higher
- PostgreSQL 11 or higher

## Features

- Cron scheduling
- Recurring Jobs
- Deferred jobs
- Automatic retries (with exponential backoff)
- Configurable job timeouts
- Direct table access for bulk loads

## Database initialization

PidginMQ requires specific database tables to function properly. To facilitate this, PidginMQ includes a command-line tool that executes the necessary migrations.

When you install the library, the CLI tool is installed automatically

CLI tool containse 3 commands:

- `up` - running next migration
- `down` - will undo last migration
- `current` - return current migration version

For all 3 commands we need to supply [database connection url](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS).

### pidginmq up

```bash
pidginmq up postgresql://other@localhost/otherdb
```

Result:

```bash
PidginMQ database schema is migrated to version: 001
```

### pidginmq down

```bash
pidginmq down postgresql://other@localhost/otherdb
```

Result:

```bash
PidginMQ database schema is downgraded to version: none
```

### pidginmq current

```bash
pidginmq down postgresql://other@localhost/otherdb
```

Result:

```bash
Current database migration version: 001
```

## Examples

Initialize the schema for PidginMQ in the database by running the CLI tool.

Create a client:

```js
    const options = {
      workers: new Workers(),
      queues: new Map<string, QueueConfig>(),
      dbConfig: {
        host: 'localhost',
        port: 5432,
        user: 'pidginmq',
        password: 'Password1',
        database: 'pidginmq',
        ssl: false,
      },
    };
    this.client = new Client(options);
    this.client.start();
```

Stop the client:

```js
this.client.stop();
```

Add a worker:

```js
this.client.addWorker('test', (job) => {
  console.info(`Processing job:`, JSON.stringify(job));
});
```

Add a job:

```js
await this.client.addJob({
  kind: 'test',
  queue: 'test-queue',
});
```

## Contributing

To setup a development environment for this library:

```bash
git clone https://github.com/ivanvs/pidginmq.git
npm install
```

To run the test suite and code coverage:

```bash
npm run test:cov
```

# License

PidginMQ is released under [MIT License](https://opensource.org/licenses/MIT).
