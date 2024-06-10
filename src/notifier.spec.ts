import { Subscription } from 'rxjs';
import { PidginException } from './exceptions/pidgin.exception.js';
import { NotificationTopic, Notifier } from './notifier.js';
import { DbDriver } from './types/db.driver.js';
import { MockProxy, any, mock } from 'jest-mock-extended';
import { Notification } from 'pg';

describe('notifier', () => {
  let notifier: Notifier;
  let driver: MockProxy<DbDriver>;

  beforeEach(() => {
    driver = mock<DbDriver>();

    notifier = new Notifier(driver);
  });

  it('should not be able to start if driver is null', () => {
    notifier = new Notifier(null);

    expect(() => notifier.start()).toThrow(PidginException);
  });

  it('should be able to start and stop', () => {
    driver.onNotification.calledWith(any()).mockReturnValue(new Subscription());
    notifier.start();

    expect(driver.onNotification).toHaveBeenCalled();
    expect(driver.listen).toHaveBeenCalled();

    notifier.stop();
    expect(driver.unlisten).toHaveBeenCalled();
  });

  it('should handle notification for job insertion', (done) => {
    let notifHandler: (notif: Notification) => void;

    driver.onNotification.calledWith(any()).mockImplementation((handler) => {
      notifHandler = handler;
      return new Subscription();
    });

    notifier.start();
    notifier.onJobInsert((message) => {
      expect(message).not.toBeNull();
      expect(message.topic).toBe(NotificationTopic.NotificationTopicInsert);
      expect(message.payload).toBe('test payload');
      notifier.stop();
      done();
    });

    notifHandler({
      channel: NotificationTopic.NotificationTopicInsert,
      payload: 'test payload',
    });
  });

  it('should handle notification for topic leadership', (done) => {
    let notifHandler: (notif: Notification) => void;

    driver.onNotification.calledWith(any()).mockImplementation((handler) => {
      notifHandler = handler;
      return new Subscription();
    });

    notifier.start();
    notifier.onLeadership((message) => {
      expect(message).not.toBeNull();
      expect(message.topic).toBe(NotificationTopic.NotificationTopicLeadership);
      expect(message.payload).toBe('test leadership');
      notifier.stop();
      done();
    });

    notifHandler({
      channel: NotificationTopic.NotificationTopicLeadership,
      payload: 'test leadership',
    });
  });

  it('should handle notification for topic job control', (done) => {
    let notifHandler: (notif: Notification) => void;

    driver.onNotification.calledWith(any()).mockImplementation((handler) => {
      notifHandler = handler;
      return new Subscription();
    });

    notifier.start();
    notifier.onJobControl((message) => {
      expect(message).not.toBeNull();
      expect(message.topic).toBe(NotificationTopic.NotificationTopicJobControl);
      expect(message.payload).toBe('test job control');
      notifier.stop();
      done();
    });

    notifHandler({
      channel: NotificationTopic.NotificationTopicJobControl,
      payload: 'test job control',
    });
  });
});
