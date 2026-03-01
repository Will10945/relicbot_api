import { Request, Response, NextFunction } from 'express';
import crypto from 'crypto';

/** Scope attached to req when API key is valid. */
export type ApiKeyScope = 'read_only' | 'read_write';

declare global {
  namespace Express {
    interface Request {
      apiKeyScope?: ApiKeyScope;
    }
  }
}

const HASH_ALGO = 'sha256';

function hashKey(key: string): string {
  return crypto.createHash(HASH_ALGO).update(key.trim()).digest('hex');
}

function parseKeysFromEnv(envValue: string | undefined): Set<string> {
  if (!envValue || typeof envValue !== 'string') return new Set();
  return new Set(
    envValue
      .split(',')
      .map((k) => k.trim())
      .filter(Boolean)
  );
}

/** Build sets of hashed keys from env. Expect API_KEYS_READ_ONLY and API_KEYS_READ_WRITE (comma-separated). */
function getKeySets(): { readOnlyHashes: Set<string>; readWriteHashes: Set<string> } {
  const readOnly = parseKeysFromEnv(process.env.API_KEYS_READ_ONLY);
  const readWrite = parseKeysFromEnv(process.env.API_KEYS_READ_WRITE);
  return {
    readOnlyHashes: new Set([...readOnly].map(hashKey)),
    readWriteHashes: new Set([...readWrite].map(hashKey)),
  };
}

let cachedKeySets: { readOnlyHashes: Set<string>; readWriteHashes: Set<string> } | null = null;

function getCachedKeySets() {
  if (!cachedKeySets) cachedKeySets = getKeySets();
  return cachedKeySets;
}

/** Extract API key from Authorization: Bearer <key> or X-API-Key header. */
function extractKey(req: Request): string | null {
  const auth = req.headers.authorization;
  if (auth && auth.startsWith('Bearer ')) return auth.slice(7).trim();
  const xKey = req.headers['x-api-key'];
  if (typeof xKey === 'string') return xKey.trim();
  return null;
}

/** Require valid API key on /api requests. Sets req.apiKeyScope; 401 if missing or invalid. */
export function apiKeyAuth(req: Request, res: Response, next: NextFunction): void {
  const key = extractKey(req);
  if (!key) {
    res.status(401).json({ error: 'Missing API key. Use Authorization: Bearer <key> or X-API-Key header.' });
    return;
  }
  const hashed = hashKey(key);
  const { readOnlyHashes, readWriteHashes } = getCachedKeySets();
  if (readWriteHashes.has(hashed)) {
    req.apiKeyScope = 'read_write';
    next();
    return;
  }
  if (readOnlyHashes.has(hashed)) {
    req.apiKeyScope = 'read_only';
    next();
    return;
  }
  res.status(401).json({ error: 'Invalid API key.' });
}
