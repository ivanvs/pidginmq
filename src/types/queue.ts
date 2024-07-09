export interface Queue {
  createdAt: Date;
  metadata: any;
  // Name is the name of the queue.
  name: string;
  // PausedAt is the time the queue was paused, if any. When a paused queue is
  // resumed, this field is set to nil.
  pausedAt: Date;
  // UpdatedAt is the last time the queue was updated. This field is updated
  // periodically any time an active Client is configured to work the queue,
  // even if the queue is paused.
  //
  // If UpdatedAt has not been updated for awhile, the queue record will be
  // deleted from the table by a maintenance process.
  updatedAt: Date;
}
