import { PidginException } from './pidgin.exception.js';

export class ValidationException extends PidginException {
  constructor(message?: string) {
    super(message);
    this.name = this.constructor.name;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
