const { Client, Workers } = require('../dist/cjs/index.js');
const { defer } = require('../dist/cjs/util/promise.js');

const jobs = parseInt('10000', 10);
const concurrency = parseInt('1000', 10);

const queues = new Map();
queues.set('task', { maxWorkers: concurrency });
const options = {
  workers: new Workers(),
  queues,
  dbConfig: {
    host: 'localhost',
    port: 5432,
    user: 'pidginmq',
    password: 'Password1',
    database: 'benchmark',
    ssl: false,
  },
};
const client = new Client(options);
client.start().then(() => {
  // A promise-based barrier.
  const reef = (n = 1) => {
    const deferred = defer();
    return {
      promise: deferred.promise,
      next() {
        --n;
        if (n < 0) return false;
        if (n === 0) deferred.resolve();
        return true;
      },
    };
  };

  const startBenchmark = (options) => {
    return new Promise((resolve) => {
      const { promise, next } = reef(options.numRuns);
      client.addWorker('benchmark_task', () => next());

      const startTime = Date.now();
      for (let i = 0; i < options.numRuns; ++i) {
        client.addJob({
          kind: 'benchmark_task',
          queue: 'task',
        });
      }

      return promise.then(() => {
        const elapsed = Date.now() - startTime;
        return client.stop().then(() => resolve(elapsed));
      });
    });
  };

  startBenchmark({
    numRuns: jobs,
    concurrency,
  }).then((time) => {
    console.log(
      `Ran ${jobs} jobs through PidginMQ with concurrency ${concurrency} in ${time} ms`,
    );
  });
});
