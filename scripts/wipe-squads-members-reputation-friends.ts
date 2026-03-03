/**
 * Wipes squads and everything built from squads (reputation, friends, host data).
 * Does NOT touch members or member-only tables.
 * Uses .env for DB connection.
 *
 * Usage: npx ts-node scripts/wipe-squads-members-reputation-friends.ts
 */

import mysql from 'mysql2/promise';
import dotenv from 'dotenv';
import * as fs from 'fs';
import * as path from 'path';

dotenv.config();

function requireEnv(name: string): string {
  const v = process.env[name];
  if (v == null || v === '') throw new Error(`Missing env: ${name}`);
  return v;
}

async function main(): Promise<void> {
  const config = {
    host: requireEnv('MYSQL_HOST'),
    user: requireEnv('MYSQL_USER'),
    password: process.env.MYSQL_PASSWORD ?? '',
    database: requireEnv('MYSQL_DATABASE'),
    multipleStatements: true as const,
  };

  const pool = mysql.createPool(config);
  const sqlPath = path.join(__dirname, 'wipe-squads-members-reputation-friends.sql');
  const sql = fs.readFileSync(sqlPath, 'utf-8');

  const conn = await pool.getConnection();
  try {
    await conn.query(sql);
    console.log('Done. Squads and squad-derived tables (reputation, friends, hosts) wiped.');
  } finally {
    conn.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
