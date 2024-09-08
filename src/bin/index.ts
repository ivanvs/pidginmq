#!/usr/bin/env node
import { Command } from 'commander';
import PackageJson from '@npmcli/package-json';
import Knex from 'knex';

const program = new Command();

program
  .name('pidginmq')
  .description('PidginMQ CLI tool for database migration')
  .version('0.0.5');

program
  .command('up')
  .description('Runs the next migration that has not yet be run.')
  .argument('<databaseUrl>', 'connection string to desired database schema')
  .action(async (databaseUrl: string) => {
    if (!databaseUrl) {
      console.error(`Database URL is not supplied`);
      process.exit(1);
    }

    const knex = Knex({
      client: 'pg',
      connection: {
        connectionString: databaseUrl,
      },
    });
    try {
      await knex.migrate.up();
      const currentVersion = await knex.migrate.currentVersion();
      console.log(
        `PidginMQ database schema is migrated to version: ${currentVersion}`,
      );
    } catch (e) {
      console.error(`${e}`);
    } finally {
      await knex.destroy();
    }
  });

program
  .command('down')
  .description('Will undo the last migration that was run.')
  .argument('<databaseUrl>', 'connection string to desired database schema')
  .action(async (databaseUrl: string) => {
    try {
      if (!databaseUrl) {
        console.error(`Database URL is not supplied`);
        process.exit(1);
      }

      const knex = Knex({
        client: 'pg',
        connection: {
          connectionString: databaseUrl,
        },
      });

      await knex.migrate.down();
      const currentVersion = await knex.migrate.currentVersion();
      console.log(
        `PidginMQ database schema is downgraded to version: ${currentVersion}`,
      );
      await knex.destroy();
    } catch (e) {
      console.error(`${e}`);
    }
  });

program
  .command('current')
  .argument('<databaseUrl>', 'connection string to desired database schema')
  .description(
    `Retrieves and returns the current migration version. If there aren't any migrations run yet, returns "none" as the value for the current version.`,
  )
  .action(async (databaseUrl: string) => {
    try {
      if (!databaseUrl) {
        console.error(`Database URL is not supplied`);
        process.exit(1);
      }

      const knex = Knex({
        client: 'pg',
        connection: {
          connectionString: databaseUrl,
        },
      });

      const currentVersion = await knex.migrate.currentVersion();
      console.log(`Current database migration version: ${currentVersion}`);
      await knex.destroy();
    } catch (e) {
      console.error(`${e}`);
    }
  });

program
  .command('version')
  .description(`Retrieves and returns the current version of PidginMQ library.`)
  .action(async () => {
    const pkgJson = new PackageJson();
    await pkgJson.load('.');
    console.log(`PidginMQ version: ${pkgJson.content.version}`);
  });

program.parseAsync(process.argv);
