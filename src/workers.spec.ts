import { Workers } from './worker.js';

describe('workers', () => {
  it('should empty on begining', () => {
    const workers = new Workers();

    expect(workers.getAllWorkers()).toStrictEqual({});
  });

  it('should be able to add handler', () => {
    const workers = new Workers();

    const demoHandler = () => {};
    workers.addWorker('test', demoHandler);

    const currentWorkers = workers.getAllWorkers();

    expect(!!currentWorkers).toBe(true);
    expect(currentWorkers['test']).not.toBeUndefined();
    expect(currentWorkers['test']).toBe(demoHandler);
  });

  it('should not to add handler when kind is undefined', () => {
    const workers = new Workers();

    const demoHandler = () => {};
    workers.addWorker(undefined, demoHandler);

    expect(workers.getAllWorkers()).toStrictEqual({});
  });

  it('should not to add handler when handler is undefined', () => {
    const workers = new Workers();

    workers.addWorker('test', undefined);

    expect(workers.getAllWorkers()).toStrictEqual({});
  });

  it('should not return handler for non-existing kind', () => {
    const workers = new Workers();

    expect(workers.getWorker('test')).toBeUndefined();
  });

  it('should return handler for kind', () => {
    const workers = new Workers();

    const demoHandler = () => {};
    workers.addWorker('test', demoHandler);

    expect(workers.getWorker('test')).toBe(demoHandler);
  });
});
