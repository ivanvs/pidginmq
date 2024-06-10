import pg, { Notification } from 'pg';

export interface DbDriver {
  open();
  close(): Promise<void>;
  execute(statement: string, ...parameters: any[]): Promise<pg.Result>;
  listen(topic: string): Promise<void>;
  unlisten(topic: string): Promise<void>;
  onNotification(handler: (notification: Notification) => void);
  get connected(): boolean;
}
