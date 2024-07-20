import { logger } from './logger/logger.settings.js';
import { Job } from './types/job.js';
import { PromiseOrDirect } from './util/util.types.js';

export type WorkerFunction = (job: Job) => PromiseOrDirect<void>;

export class PidginWorker {
  constructor(private handler: WorkerFunction) {}

  nextRetry() {
    return 0;
  }

  work(job: Job): PromiseOrDirect<void> {
    if (this.handler) {
      return this.handler(job);
    }

    return null;
  }
}

interface WorkersDictionary {
  [kind: string]: WorkerFunction;
}

export class Workers {
  private workers: WorkersDictionary = {};

  addWorker(kind: string, handler: WorkerFunction) {
    if (!kind || !handler) {
      logger.warn('Worker do not have handler or kind', kind, handler);
      return;
    }

    if (!this.workers[kind]) {
      logger.warn(`Worker for kind already exists: ${kind}`);
    }

    this.workers[kind] = handler;
  }

  getWorker(kind: string): WorkerFunction {
    return this.workers[kind];
  }

  getAllWorkers() {
    return { ...this.workers };
  }
}
