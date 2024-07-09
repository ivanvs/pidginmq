import { PostgreSqlContainer } from '@testcontainers/postgresql';

export const getTestContainer = () => {
  return new PostgreSqlContainer().withCopyFilesToContainer([
    {
      source: 'migration/01_create_initial_schema.sql',
      target: '/docker-entrypoint-initdb.d/01-schema.sql',
    },
  ]);
};
