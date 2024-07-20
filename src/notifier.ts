import { Subject, Subscription } from 'rxjs';
import { DbDriver } from './types/db.driver.js';
import { Notification } from 'pg';
import { PidginException } from './exceptions/pidgin.exception.js';
import { logger } from './logger/logger.settings.js';

export enum NotificationTopic {
  NotificationTopicInsert = 'pidginmq_insert',
  NotificationTopicLeadership = 'pidginmq_leadership',
  NotificationTopicJobControl = 'pidginmq_job_control',
}

export type NotifyFunction = (notification: DbNotification) => void;

export interface DbNotification {
  topic: NotificationTopic;
  payload: string;
}

export class Notifier {
  private driverSubscription: Subscription;
  private jobInsertSubject: Subject<DbNotification>;
  private leadershipSubject: Subject<DbNotification>;
  private jobControlSubject: Subject<DbNotification>;

  constructor(private driver: DbDriver) {}

  start() {
    logger.info('Starting notifier');
    if (!this.driver) {
      logger.error(`The DB driver is not set`);
      throw new PidginException('Driver is not set');
    }

    if (!this.driverSubscription) {
      this.jobInsertSubject = new Subject<DbNotification>();
      this.leadershipSubject = new Subject<DbNotification>();
      this.jobControlSubject = new Subject<DbNotification>();

      this.driverSubscription = this.driver.onNotification(
        this.notificationHandler,
      );

      this.driver.listen(NotificationTopic.NotificationTopicInsert);
    }
  }

  private notificationHandler = (notification: Notification) => {
    const { payload, channel } = notification;

    const notifValues = Object.values(NotificationTopic);
    const topic = notifValues.find((x) => x === channel);

    if (topic) {
      const newNotif = { topic, payload };
      switch (topic) {
        case NotificationTopic.NotificationTopicInsert:
          return this.jobInsertSubject?.next(newNotif);
        case NotificationTopic.NotificationTopicLeadership:
          return this.leadershipSubject?.next(newNotif);
        case NotificationTopic.NotificationTopicJobControl:
          return this.jobControlSubject?.next(newNotif);
      }
    }
  };

  stop() {
    logger.info('Stopping notifier');
    if (this.driverSubscription) {
      this.driver.unlisten(NotificationTopic.NotificationTopicInsert);
      this.driverSubscription?.unsubscribe();
      this.driverSubscription = null;

      this.jobControlSubject?.complete();
      this.jobControlSubject = null;

      this.leadershipSubject?.complete();
      this.leadershipSubject = null;

      this.jobInsertSubject?.complete();
      this.jobInsertSubject = null;
    }
  }

  onJobInsert(handler: NotifyFunction): Subscription {
    return this.jobInsertSubject?.subscribe(handler);
  }

  onLeadership(handler: NotifyFunction): Subscription {
    return this.leadershipSubject?.subscribe(handler);
  }

  onJobControl(handler: NotifyFunction): Subscription {
    return this.jobControlSubject?.subscribe(handler);
  }
}
