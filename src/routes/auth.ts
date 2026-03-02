import express from 'express';
import bcrypt from 'bcrypt';
import Joi from 'joi';
import {
  getUserByUsername,
  getUserById,
  getUserByMemberId,
  createUser,
  createSession,
  deleteSessionById,
  getMemberById,
  resolveMember,
  updateUserMemberLink,
} from '../database/database';
import { sessionAuth, SESSION_COOKIE_NAME } from '../middleware/sessionAuth';

function toMemberPayload(member: { MemberID?: number; MemberName?: string; DiscordID?: number | string } | null) {
  if (!member) return null;
  return {
    id: member.MemberID,
    name: member.MemberName,
    discordId: member.DiscordID != null ? String(member.DiscordID) : undefined,
  };
}

const router = express.Router();

const SESSION_DURATION_MS = 365 * 24 * 60 * 60 * 1000; // 1 year

/** POST /auth/register — body: { username, password, memberId?, discordId?, memberName? }. Creates account, optional member link. Returns { user, expiresAt } and sets session cookie. */
const registerSchema = Joi.object({
  username: Joi.string().required(),
  password: Joi.string().required(),
  memberId: Joi.number().optional(),
  discordId: Joi.alternatives().try(Joi.string(), Joi.number()).optional(),
  memberName: Joi.string().optional(),
});

router.post('/register', async (req, res) => {
  const { error, value } = registerSchema.validate(req.body);
  if (error) {
    return res.status(400).json({ error: error.details.map((d) => d.message).join('; ') });
  }
  const existing = await getUserByUsername(value.username);
  if (existing) {
    return res.status(409).json({ error: 'Username already exists.' });
  }
  let memberIdToLink: number | null = null;
  const hasMemberLink =
    value.memberId != null ||
    (value.discordId != null && (typeof value.discordId !== 'string' || value.discordId.trim() !== '')) ||
    (value.memberName != null && String(value.memberName).trim() !== '');
  if (hasMemberLink) {
    const member = await resolveMember({
      memberId: value.memberId,
      discordId: value.discordId != null ? String(value.discordId) : undefined,
      memberName: value.memberName,
    });
    if (!member) {
      return res.status(404).json({ error: 'Member not found.' });
    }
    memberIdToLink = member.MemberID ?? null;
    if (memberIdToLink != null) {
      const existingLink = await getUserByMemberId(memberIdToLink);
      if (existingLink) {
        return res.status(409).json({ error: 'Member is already linked to another account.' });
      }
    }
  }
  const passwordHash = await bcrypt.hash(value.password, 10);
  const userId = await createUser(value.username, passwordHash);
  if (memberIdToLink != null) {
    await updateUserMemberLink(userId, memberIdToLink);
  }
  const expiresAt = Math.floor((Date.now() + SESSION_DURATION_MS) / 1000);
  const sessionId = await createSession(userId, expiresAt);
  const isProd = process.env.NODE_ENV === 'production';
  res.cookie(SESSION_COOKIE_NAME, sessionId, {
    httpOnly: true,
    secure: isProd,
    sameSite: isProd ? 'lax' : 'strict',
    maxAge: SESSION_DURATION_MS,
    path: '/',
  });
  const user = await getUserById(userId);
  const member = user?.member_id ? (await getMemberById(user.member_id))[0] ?? null : null;
  res.status(201).json({
    user: {
      id: userId,
      username: value.username,
      member: toMemberPayload(member),
    },
    expiresAt,
  });
});

/** POST /auth/login — body: { username, password }. Returns { user: { id, username, member? }, expiresAt }. Sets session cookie. */
router.post('/login', async (req, res) => {
  const schema = Joi.object({
    username: Joi.string().required(),
    password: Joi.string().required(),
  });
  const { error, value } = schema.validate(req.body);
  if (error) {
    return res.status(400).json({ error: error.details.map((d) => d.message).join('; ') });
  }
  const user = await getUserByUsername(value.username);
  if (!user) {
    return res.status(401).json({ error: 'Invalid username or password.' });
  }
  const match = await bcrypt.compare(value.password, user.password_hash);
  if (!match) {
    return res.status(401).json({ error: 'Invalid username or password.' });
  }
  const expiresAt = Math.floor((Date.now() + SESSION_DURATION_MS) / 1000);
  const sessionId = await createSession(user.id, expiresAt);
  const isProd = process.env.NODE_ENV === 'production';
  res.cookie(SESSION_COOKIE_NAME, sessionId, {
    httpOnly: true,
    secure: isProd,
    sameSite: isProd ? 'lax' : 'strict',
    maxAge: SESSION_DURATION_MS,
    path: '/',
  });
  const member = user.member_id
    ? (await getMemberById(user.member_id))[0] ?? null
    : null;
  res.status(200).json({
    user: {
      id: user.id,
      username: user.username,
      member: toMemberPayload(member),
    },
    expiresAt,
  });
});

/** POST /auth/logout — invalidates current session. Requires session (cookie or Bearer). */
router.post('/logout', sessionAuth, async (req, res) => {
  const token = req.cookies?.[SESSION_COOKIE_NAME] ?? req.headers.authorization?.replace(/^Bearer\s+/i, '').trim();
  if (token) await deleteSessionById(token);
  res.clearCookie(SESSION_COOKIE_NAME, { path: '/' });
  res.status(200).json({ ok: true });
});

/** GET /auth/me — returns current user (and linked member if any) if session valid, else 401. */
router.get('/me', sessionAuth, async (req, res) => {
  if (!req.userId || !req.user) {
    return res.status(401).json({ error: 'Not authenticated.' });
  }
  const user = await getUserById(req.userId);
  if (!user) {
    return res.status(401).json({ error: 'Not authenticated.' });
  }
  const member = user.member_id
    ? (await getMemberById(user.member_id))[0] ?? null
    : null;
  res.json({
    user: {
      id: user.id,
      username: user.username,
      member: toMemberPayload(member),
    },
  });
});

/** PUT /auth/me/link — link current user to a member by id, discordId, or memberName. Body: { memberId?, discordId?, memberName? } (exactly one). */
const linkSchema = Joi.object({
  memberId: Joi.number().optional(),
  discordId: Joi.alternatives().try(Joi.string(), Joi.number()).optional(),
  memberName: Joi.string().optional(),
}).or('memberId', 'discordId', 'memberName');

router.put('/me/link', sessionAuth, async (req, res) => {
  if (!req.userId || !req.user) {
    return res.status(401).json({ error: 'Not authenticated.' });
  }
  const { error, value } = linkSchema.validate(req.body);
  if (error) {
    return res.status(400).json({
      error: 'Provide exactly one of: memberId, discordId, or memberName.',
      details: error.details.map((d) => d.message).join('; '),
    });
  }
  const member = await resolveMember({
    memberId: value.memberId,
    discordId: value.discordId != null ? String(value.discordId) : undefined,
    memberName: value.memberName,
  });
  if (!member) {
    return res.status(404).json({ error: 'Member not found.' });
  }
  const memberId = member.MemberID ?? null;
  if (memberId != null) {
    const existingLink = await getUserByMemberId(memberId);
    if (existingLink && existingLink.id !== req.userId) {
      return res.status(409).json({ error: 'Member is already linked to another account.' });
    }
  }
  await updateUserMemberLink(req.userId, memberId);
  const user = await getUserById(req.userId);
  const linked = user?.member_id ? (await getMemberById(user.member_id))[0] ?? null : null;
  res.json({
    user: {
      id: req.user!.id,
      username: req.user!.username,
      member: toMemberPayload(linked),
    },
  });
});

/** DELETE /auth/me/link — remove the member link for the current user. */
router.delete('/me/link', sessionAuth, async (req, res) => {
  if (!req.userId || !req.user) {
    return res.status(401).json({ error: 'Not authenticated.' });
  }
  await updateUserMemberLink(req.userId, null);
  res.json({
    user: {
      id: req.user.id,
      username: req.user.username,
      member: null,
    },
  });
});

export default router;
