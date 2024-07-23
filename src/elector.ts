import { Subject } from 'rxjs';
import { Executor } from './executor.js';
import { DbNotification, NotificationTopic, Notifier } from './notifier.js';
import { sleep } from './util/promise.js';
import { DateTime } from 'luxon';
import { logger } from './logger/logger.settings.js';

interface LeadershipNotification {
  name: string;
  leaderID: string;
  action: string;
}

export interface LeadershipEvent {
  isLeader: boolean;
  time: DateTime;
}

export type LeadershipEventHandler = (LeadershipEvent) => void;

export class Elector {
  private executor: Executor;
  private id: string;
  private interval: number;
  private name: string;
  private ttl: number;
  private isLeader: boolean;
  private notifier: Notifier;

  private timer: NodeJS.Timeout;

  private leadershipSubject: Subject<LeadershipEvent>;

  constructor(
    executor: Executor,
    id: string,
    name: string,
    interval: number,
    ttl: number,
    notifier: Notifier,
  ) {
    this.executor = executor;
    this.id = id;
    this.name = name;
    this.interval = interval;
    this.ttl = interval + ttl;
    this.notifier = notifier;

    this.leadershipSubject = new Subject<LeadershipEvent>();
  }

  run() {
    logger.info(`Elector is started`);
    this.notifier.onLeadership((notification) =>
      this.handleLeadershipMessage(notification),
    );
    this.timer = setInterval(
      async () => await this.intervalHandler(),
      this.interval,
    );
  }

  subscribe(handler: LeadershipEventHandler) {
    return this.leadershipSubject.subscribe(handler);
  }

  async stop() {
    logger.info('Elector is being stopped');
    await this.giveUpLeadership();
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    if (this.leadershipSubject) {
      this.leadershipSubject.complete();
      this.leadershipSubject = null;
    }
    logger.info('Elector is stopped');
  }

  private async intervalHandler() {
    if (!this.isLeader) {
      return await this.gainLeadership();
    } else {
      return await this.keepLeadership();
    }
  }

  private handleLeadershipMessage(notification: DbNotification) {
    if (notification.topic !== NotificationTopic.NotificationTopicLeadership) {
      logger.debug(
        `Notification is not TopicLeadership: ${notification.topic}`,
      );
      return;
    }

    const parsedNotification: LeadershipNotification = JSON.parse(
      notification.payload,
    );

    if (
      parsedNotification.action !== 'resigned' ||
      parsedNotification.name != this.name
    )
      // We only care about resignations on because we use them to preempt the
      // election attempt backoff. And we only care about our own key name.
      return;
    return;
  }

  async gainLeadership() {
    try {
      logger.debug(`Requesting leadership`);
      const elected = await this.attemptElectOrReelect(false);
      if (elected) {
        logger.debug('Leadership gained');
        this.isLeader = true;
        this.leadershipSubject?.next({ isLeader: true, time: DateTime.utc() });
      }

      return elected;
    } catch (e) {
      logger.error(`Gain leadership failed: `, e);
      return false;
    }
  }

  async keepLeadership() {
    try {
      logger.trace('Requesting to keep leadership');
      const reelected = await this.attemptElectOrReelect(true);

      if (!reelected) {
        logger.info(`Elector is not reelected`);
        this.leadershipSubject?.next({ isLeader: false, time: DateTime.utc() });
      }
    } catch (e) {
      logger.error(`Keep leadership failed: `, e);
    }
  }

  async giveUpLeadership() {
    for (let i = 0; i < 10; i++) {
      try {
        await this.attemptResign(i);
        this.leadershipSubject?.next({ isLeader: false, time: DateTime.utc() });
        return;
      } catch (e) {
        logger.error('Give up leadership failed: ', e);
      }
    }
  }

  async attemptResign(attempt: number) {
    await sleep((attempt + 1) * 1_000);
    await this.executor.reasignLeader({
      leaderID: this.id,
      leadershipTopic: NotificationTopic.NotificationTopicLeadership,
      name: this.name,
    });
  }

  async attemptElectOrReelect(alreadyElected: boolean): Promise<boolean> {
    //TODO execute this in transaction
    await this.executor.deleteExpiredLeader(this.name);

    let result = 0;
    if (alreadyElected) {
      result = await this.executor.attemptReelectLeader({
        leaderID: this.id,
        name: this.name,
        ttl: this.ttl,
      });
    } else {
      result = await this.executor.attemptElectLeader({
        leaderID: this.id,
        name: this.name,
        ttl: this.ttl,
      });
    }

    return result !== 0;
  }
}
