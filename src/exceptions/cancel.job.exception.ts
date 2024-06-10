import { PidginException } from './pidgin.exception.js';

export class CancelJobException extends PidginException {
  constructor(
    private id: number,
    message?: string,
  ) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }

  public get ID(): number {
    return this.id;
  }
}
