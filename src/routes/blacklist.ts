import express from 'express';
import {
    getGlobalBlacklistMemberIds,
    getMemberBlacklistsByMemberIds,
    addGlobalBlacklist,
    removeGlobalBlacklist,
    addMemberBlacklistEntries,
    removeMemberBlacklistEntries
} from '../database/database';

const router = express.Router();

const BULK_IDS_MAX = 100;

/** Parse member IDs from query or body. Returns null if invalid. */
function parseMemberIdsFromRequest(req: express.Request): number[] | null {
    const raw = req.body?.ids ?? req.query.ids;
    if (raw === undefined || raw === null) return null;
    const arr = Array.isArray(raw) ? raw : (typeof raw === 'string' ? raw.split(',').map((s) => s.trim()) : [raw]);
    const ids: number[] = [];
    for (const v of arr) {
        const n = typeof v === 'string' ? parseInt(v, 10) : Number(v);
        if (!Number.isInteger(n) || n < 1) return null;
        ids.push(n);
    }
    return ids.length === 0 ? null : [...new Set(ids)];
}

/** GET /api/blacklist/global — list of globally blacklisted member IDs. */
router.get('/global', async (req, res) => {
    try {
        const memberIds = await getGlobalBlacklistMemberIds();
        res.json({ memberIds });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** POST /api/blacklist/global — bulk add/remove. Body: { add?: number[], remove?: number[] }. */
router.post('/global', async (req, res) => {
    try {
        const add = req.body?.add;
        const remove = req.body?.remove;
        const toAdd = Array.isArray(add) ? add.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n) && n >= 1) : [];
        const toRemove = Array.isArray(remove) ? remove.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n) && n >= 1) : [];
        if (toAdd.length > 0) await addGlobalBlacklist(toAdd);
        if (toRemove.length > 0) await removeGlobalBlacklist(toRemove);
        const memberIds = await getGlobalBlacklistMemberIds();
        res.json({ memberIds });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** GET /api/blacklist?ids=1,2,3 — per-member blacklists for given member IDs. */
router.get('/', async (req, res) => {
    try {
        const ids = parseMemberIdsFromRequest(req);
        if (!ids) return res.status(400).json({ error: 'Invalid or missing ids; use query ?ids=1,2,3' });
        if (ids.length > BULK_IDS_MAX) return res.status(400).json({ error: `Too many ids; maximum ${BULK_IDS_MAX}` });
        const map = await getMemberBlacklistsByMemberIds(ids);
        const results = ids.map((memberId) => ({ memberId, blacklistedMemberIds: map.get(memberId) ?? [] }));
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** POST /api/blacklist — bulk read (body { ids }) or bulk edit (body { entries: [ { memberId, add?, remove? } ] }). */
router.post('/', async (req, res) => {
    try {
        const entries = req.body?.entries;
        if (Array.isArray(entries) && entries.length > 0) {
            if (entries.length > BULK_IDS_MAX) return res.status(400).json({ error: `Too many entries; maximum ${BULK_IDS_MAX}` });
            const toAdd: { memberId: number; blacklistedMemberIds: number[] }[] = [];
            const toRemove: { memberId: number; blacklistedMemberIds: number[] }[] = [];
            const memberIds: number[] = [];
            for (const e of entries) {
                const memberId = typeof e.memberId === 'number' ? e.memberId : parseInt(String(e.memberId), 10);
                if (!Number.isInteger(memberId) || memberId < 1) continue;
                memberIds.push(memberId);
                if (Array.isArray(e.add) && e.add.length) toAdd.push({ memberId, blacklistedMemberIds: e.add.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n)) });
                if (Array.isArray(e.remove) && e.remove.length) toRemove.push({ memberId, blacklistedMemberIds: e.remove.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n)) });
            }
            const affectedIds = [...new Set(memberIds)];
            await addMemberBlacklistEntries(toAdd);
            await removeMemberBlacklistEntries(toRemove);
            const map = await getMemberBlacklistsByMemberIds(affectedIds);
            const results = affectedIds.map((memberId) => ({ memberId, blacklistedMemberIds: map.get(memberId) ?? [] }));
            return res.json({ results });
        }
        const ids = parseMemberIdsFromRequest(req);
        if (!ids) return res.status(400).json({ error: 'Invalid or missing ids; send JSON body { "ids": [1, 2, 3] }' });
        if (ids.length > BULK_IDS_MAX) return res.status(400).json({ error: `Too many ids; maximum ${BULK_IDS_MAX}` });
        const map = await getMemberBlacklistsByMemberIds(ids);
        const results = ids.map((memberId) => ({ memberId, blacklistedMemberIds: map.get(memberId) ?? [] }));
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

export default router;
