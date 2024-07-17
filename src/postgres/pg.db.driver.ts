import { DbDriver } from 'src/types/db.driver.js';
import { PidginMqLogger } from '../logger/logger.js';
import pg, { Pool, Notification, Client } from 'pg';
import { Subject, Subscription } from 'rxjs';

/** Configuration for connecting to PostgreSQL */
export interface PostgresDbOptions {
  /** PostgreSQL IP address[s] or domain name[s] */
  host: string;
  /** PostgreSQL server port */
  port: number;
  /** Username of databse user*/
  user: string;
  /** Password of database user*/
  password: string;
  /** Database name */
  database: string;
  /** Is SSL enalled or not */
  ssl: boolean;
}

export class PostgresDbDriver implements DbDriver {
  private connection: Pool;
  private eventSubject: Subject<Notification>;
  private notificationClient: Client;

  constructor(
    private options: PostgresDbOptions,
    private logger: PidginMqLogger,
  ) {}

  get connected(): boolean {
    return !!this.connection;
  }

  async open() {
    if (!this.connection) {
      this.logger.debug('Opening new connection to databse');
      this.connection = new Pool(this.options);
      await this.execute('SELECT 1');

      this.notificationClient = new Client(this.options);
      await this.notificationClient.connect();
    }

    if (!this.eventSubject) {
      this.eventSubject = new Subject<Notification>();
    }
  }

  async close(): Promise<void> {
    if (this.connection) {
      this.logger.debug('Closing connection to database');
      this.notificationClient.end();
      this.notificationClient = null;

      await this.connection.end();
      this.connection = null;
    }

    if (this.eventSubject) {
      this.eventSubject.complete();
      this.eventSubject = null;
    }
  }

  async execute(statment: string, ...parameters: any[]): Promise<pg.Result> {
    if (parameters.length > 0) {
      return await this.connection.query(statment, parameters);
    } else {
      return await this.connection.query(statment);
    }
  }

  async listen(topic: string): Promise<void> {
    await this.notificationClient.query(`LISTEN ${topic};`);
    this.notificationClient.on('notification', (msg) => {
      if (this.eventSubject) {
        this.eventSubject.next(msg);
      }
    });
  }

  unlisten(topic: string): Promise<void> {
    return this.notificationClient.query(`UNLISTEN ${topic};`);
  }

  onNotification(handler: (notification: Notification) => void): Subscription {
    return this.eventSubject?.subscribe(handler);
  }
}
