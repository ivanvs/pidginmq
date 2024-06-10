export interface PidginMqLogger {
  debug(message?: any, ...optionalParams: any[]): void;
  info(message?: any, ...optionalParams: any[]): void;
  warning(message?: any, ...optionalParams: any[]): void;
  error(message?: any, ...optionalParams: any[]): void;
}

class NoLoggerImplementation implements PidginMqLogger {
  debug(): void {}
  info(): void {}
  warning(): void {}
  error(): void {}
}

class ConsoleLoggerImplementation implements PidginMqLogger {
  debug(message?: any, ...optionalParams: any[]): void {
    console.log(message, optionalParams);
  }
  info(message?: any, ...optionalParams: any[]): void {
    console.info(message, optionalParams);
  }
  warning(message?: any, ...optionalParams: any[]): void {
    console.warn(message, optionalParams);
  }
  error(message?: any, ...optionalParams: any[]): void {
    console.error(message, optionalParams);
  }
}
export const ConsoleLogger = new ConsoleLoggerImplementation();
export const NoLogger = new NoLoggerImplementation();
