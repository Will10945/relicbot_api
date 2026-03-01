/**
 * One-off script to create an auth user (for login). Run after migrations.
 * Usage: npx ts-node src/scripts/create-user.ts <username> <password>
 * Or set CREATE_USER_NAME and CREATE_USER_PASSWORD in env.
 */
import bcrypt from 'bcrypt';
import dotenv from 'dotenv';
import { createUser, getUserByUsername } from '../database/database';

dotenv.config();

const BCRYPT_ROUNDS = 10;

async function main(): Promise<void> {
  const username = process.argv[2] ?? process.env.CREATE_USER_NAME;
  const password = process.argv[3] ?? process.env.CREATE_USER_PASSWORD;
  if (!username || !password) {
    console.error('Usage: npx ts-node src/scripts/create-user.ts <username> <password>');
    process.exit(1);
  }
  const existing = await getUserByUsername(username);
  if (existing) {
    console.error(`User "${username}" already exists.`);
    process.exit(1);
  }
  const hash = await bcrypt.hash(password, BCRYPT_ROUNDS);
  const id = await createUser(username, hash);
  console.log(`Created user id=${id} username=${username}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
