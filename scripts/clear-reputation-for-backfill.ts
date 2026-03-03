/**
 * Clears reputation and friends tables so backfill-reputation will process all closed squads from scratch.
 * Does NOT touch squads or any squad data. Uses .env for DB connection.
 *
 * Usage: npx ts-node scripts/clear-reputation-for-backfill.ts
 * Or: npm run clear-reputation-for-backfill
 *
 * Then run: npm run backfill-reputation
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
  const sqlPath = path.join(__dirname, 'clear-reputation-for-backfill.sql');
  const sql = fs.readFileSync(sqlPath, 'utf-8');

  const conn = await pool.getConnection();
  try {
    await conn.query(sql);
    console.log('Done. Reputation and friends tables cleared. Run: npm run backfill-reputation');
  } finally {
    conn.release();
    await pool.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
