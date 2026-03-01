import { Request, Response, NextFunction } from 'express';

/** Reject write methods (POST, PUT, PATCH, DELETE) when scope is read_only. Call after apiKeyAuth. */
export function requireScope(req: Request, res: Response, next: NextFunction): void {
  const scope = req.apiKeyScope;
  if (scope === 'read_write') {
    next();
    return;
  }
  if (scope === 'read_only') {
    const method = req.method.toUpperCase();
    if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) {
      res.status(403).json({ error: 'API key has read-only scope. Write operations are not allowed.' });
      return;
    }
  }
  next();
}
