import express from 'express';
import {
    getSubscriptionsBundle,
    addRelicSubscriptions,
    removeRelicSubscriptions,
    addMemberSubscriptions,
    removeMemberSubscriptions,
    addHostSubscriptions,
    removeHostSubscriptions
} from '../database/database';

const router = express.Router();

const BULK_IDS_MAX = 100;

/** Parse member IDs from query (?ids=1,2,3) or body (POST { ids: [1,2,3] }). Returns null if invalid. */
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

/** GET /api/subscriptions?ids=1,2,3 — bulk read subscriptions (relics, members, hosts) for given member IDs. */
router.get('/', async (req, res) => {
    try {
        const ids = parseMemberIdsFromRequest(req);
        if (!ids) return res.status(400).json({ error: 'Invalid or missing ids; use query ?ids=1,2,3' });
        if (ids.length > BULK_IDS_MAX) return res.status(400).json({ error: `Too many ids; maximum ${BULK_IDS_MAX}` });
        const results = await getSubscriptionsBundle(ids);
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** POST /api/subscriptions with body { "ids": [1,2,3] } — same as GET, for bulk read. */
router.post('/', async (req, res) => {
    try {
        const ids = req.body?.ids;
        if (!Array.isArray(ids) || ids.length === 0) {
            const parsed = parseMemberIdsFromRequest(req);
            if (parsed && parsed.length > 0) {
                if (parsed.length > BULK_IDS_MAX) return res.status(400).json({ error: `Too many ids; maximum ${BULK_IDS_MAX}` });
                const results = await getSubscriptionsBundle(parsed);
                return res.json({ results });
            }
            return res.status(400).json({ error: 'Invalid or missing ids; send JSON body { "ids": [1, 2, 3] }' });
        }
        const numIds = ids.map((v: unknown) => (typeof v === 'string' ? parseInt(v, 10) : Number(v))).filter((n: number) => Number.isInteger(n) && n >= 1);
        if (numIds.length === 0) return res.status(400).json({ error: 'Invalid ids' });
        const uniqueIds = [...new Set(numIds)];
        if (uniqueIds.length > BULK_IDS_MAX) return res.status(400).json({ error: `Too many ids; maximum ${BULK_IDS_MAX}` });
        const results = await getSubscriptionsBundle(uniqueIds);
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** POST /api/subscriptions/edit — bulk add/remove. Body: { entries: [ { memberId, relics?: { add?, remove? }, members?: { add?, remove? }, hosts?: { add?: string[], remove?: number[] } } ] }. Returns updated state for affected member IDs. */
router.post('/edit', async (req, res) => {
    try {
        const entries = req.body?.entries;
        if (!Array.isArray(entries) || entries.length === 0) {
            return res.status(400).json({ error: 'Send JSON body { "entries": [ { "memberId": 1, "relics": { "add": [], "remove": [] }, ... } ] }' });
        }
        if (entries.length > BULK_IDS_MAX) return res.status(400).json({ error: `Too many entries; maximum ${BULK_IDS_MAX}` });
        const memberIds: number[] = [];
        const relicAdds: { memberId: number; relicIds: number[] }[] = [];
        const relicRemoves: { memberId: number; relicIds: number[] }[] = [];
        const memberAdds: { memberId: number; memberIds: number[] }[] = [];
        const memberRemoves: { memberId: number; memberIds: number[] }[] = [];
        const hostAdds: { memberId: number; hostSignatures: string[] }[] = [];
        const hostRemoves: { memberId: number; hostIds: number[] }[] = [];
        for (const e of entries) {
            const memberId = typeof e.memberId === 'number' ? e.memberId : parseInt(String(e.memberId), 10);
            if (!Number.isInteger(memberId) || memberId < 1) continue;
            memberIds.push(memberId);
            if (e.relics) {
                if (Array.isArray(e.relics.add) && e.relics.add.length) relicAdds.push({ memberId, relicIds: e.relics.add.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n)) });
                if (Array.isArray(e.relics.remove) && e.relics.remove.length) relicRemoves.push({ memberId, relicIds: e.relics.remove.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n)) });
            }
            if (e.members) {
                if (Array.isArray(e.members.add) && e.members.add.length) memberAdds.push({ memberId, memberIds: e.members.add.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n)) });
                if (Array.isArray(e.members.remove) && e.members.remove.length) memberRemoves.push({ memberId, memberIds: e.members.remove.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n)) });
            }
            if (e.hosts) {
                if (Array.isArray(e.hosts.add) && e.hosts.add.length) hostAdds.push({ memberId, hostSignatures: e.hosts.add.map((x: unknown) => String(x)) });
                if (Array.isArray(e.hosts.remove) && e.hosts.remove.length) hostRemoves.push({ memberId, hostIds: e.hosts.remove.map((x: unknown) => Number(x)).filter((n: number) => Number.isInteger(n)) });
            }
        }
        const affectedIds = [...new Set(memberIds)];
        await Promise.all([
            addRelicSubscriptions(relicAdds),
            removeRelicSubscriptions(relicRemoves),
            addMemberSubscriptions(memberAdds),
            removeMemberSubscriptions(memberRemoves),
            addHostSubscriptions(hostAdds),
            removeHostSubscriptions(hostRemoves)
        ]);
        const results = await getSubscriptionsBundle(affectedIds);
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

export default router;
