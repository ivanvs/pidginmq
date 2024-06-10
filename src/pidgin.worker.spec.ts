import { PidginWorker } from './worker.js';

describe('pidgin worker', () => {
  it('should call handler', () => {
    const demoHandler = jest.fn(() => {});
    const worker = new PidginWorker(demoHandler);

    expect(() => worker.work(null)).not.toThrow();
    expect(demoHandler.mock.calls).toHaveLength(1);
  });

  it('should not call handler if handler is non-existing', () => {
    const worker = new PidginWorker(null);

    expect(worker.work(null)).toBeNull();
  });
});
