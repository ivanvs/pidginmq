import { PidginException } from './pidgin.exception.js';

export class SnoozeJobException extends PidginException {
  constructor(
    private durationInSeconds: number,
    message?: string,
  ) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }

  public get durationSeconds(): number {
    return this.durationInSeconds;
  }
}
