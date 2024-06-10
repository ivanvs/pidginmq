import { PidginException } from './pidgin.exception.js';

export class TimedoutJobException extends PidginException {
  constructor() {
    super('Timed out');
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
