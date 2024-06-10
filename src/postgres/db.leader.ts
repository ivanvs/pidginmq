export interface DbLeader {
  elected_at: Date;
  expires_at: Date;
  leader_id: string;
  name: string;
}
