import { Request, Response, NextFunction } from 'express';
import { getSessionById, getUserById } from '../database/database';

export const SESSION_COOKIE_NAME = 'session_id';

/** Optional session: sets req.userId and req.user if valid session present. Does not 401. */
declare global {
  namespace Express {
    interface Request {
      userId?: number;
      user?: { id: number; username: string };
    }
  }
}

export async function sessionAuth(req: Request, res: Response, next: NextFunction): Promise<void> {
  let token: string | undefined;
  const cookie = req.cookies?.[SESSION_COOKIE_NAME];
  if (cookie) token = cookie;
  const auth = req.headers.authorization;
  if (!token && auth?.startsWith('Bearer ')) token = auth.slice(7).trim();
  if (!token) {
    next();
    return;
  }
  try {
    const session = await getSessionById(token);
    if (!session) {
      next();
      return;
    }
    req.userId = session.user_id;
    const user = await getUserById(session.user_id);
    if (user) req.user = { id: user.id, username: user.username };
  } catch (_) {
    // leave req.userId unset
  }
  next();
}
