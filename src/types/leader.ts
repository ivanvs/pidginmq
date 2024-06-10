export interface Leader {
  electedAt: Date;
  expiresAt: Date;
  leaderID: string;
  name: string;
}
