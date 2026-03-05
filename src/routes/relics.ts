import express from 'express';
import {
    getAllRelics,
    getRelic,
    getRelicProfileData,
    getRelicDrops,
    getRelicDropsByRelicIds,
    getProfileOrderBy,
    parseFilledOnlyParam,
    parseDateRange,
    normalizeFromTo
} from '../database/database';

const router = express.Router();

const relicDebugger = require('debug')('app:relicsEndpoint');

router.get('/', async (req, res) => {
    try {
        const relics = await getAllRelics();
        const ids = relics.map((r) => r.ID);
        const dropsMap = await getRelicDropsByRelicIds(ids);
        const results = relics.map((r) => ({
            ...r,
            drops: dropsMap.get(r.ID) ?? []
        }));
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** Parse relic IDs from query (?ids=1,2,3 or ?ids=1&ids=2) or body (POST { ids: [1,2,3] }). Returns null if invalid. */
function parseRelicIdsFromRequest(req: express.Request): number[] | null {
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

router.get('/profiles', async (req, res) => {
    try {
        const ids = parseRelicIdsFromRequest(req);
        if (!ids) return res.status(400).json({ error: 'Invalid or missing ids; use query ?ids=1,2,3' });
        const sortBy = getProfileOrderBy((req.query.sort as string) ?? '');
        const filledOnly = parseFilledOnlyParam(req.query.all);
        const { from, to } = normalizeFromTo(req.query as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const [profilesData, dropsMap] = await Promise.all([
            Promise.all(ids.map((id) => getRelicProfileData(id, sortBy, filledOnly, dateRange))),
            getRelicDropsByRelicIds(ids)
        ]);
        const results = profilesData
            .filter((data) => data.relic != null)
            .map((data) => ({ ...data, relic: { ...data.relic!, drops: dropsMap.get(data.relic!.id) ?? [] } }));
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.post('/profiles', async (req, res) => {
    try {
        const ids = parseRelicIdsFromRequest(req);
        if (!ids) return res.status(400).json({ error: 'Invalid or missing ids; send JSON body { "ids": [1, 2, 3] }' });
        const sortBy = getProfileOrderBy((req.query.sort as string) ?? req.body?.sort ?? '');
        const filledOnly = parseFilledOnlyParam(req.query.all ?? req.body?.all);
        const { from, to } = normalizeFromTo({ ...req.query, ...req.body } as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const [profilesData, dropsMap] = await Promise.all([
            Promise.all(ids.map((id) => getRelicProfileData(id, sortBy, filledOnly, dateRange))),
            getRelicDropsByRelicIds(ids)
        ]);
        const results = profilesData
            .filter((data) => data.relic != null)
            .map((data) => ({ ...data, relic: { ...data.relic!, drops: dropsMap.get(data.relic!.id) ?? [] } }));
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:id/profile', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid relic ID' });
        const sortBy = getProfileOrderBy((req.query.sort as string) ?? '');
        const filledOnly = parseFilledOnlyParam(req.query.all);
        const { from, to } = normalizeFromTo(req.query as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const [data, drops] = await Promise.all([
            getRelicProfileData(id, sortBy, filledOnly, dateRange),
            getRelicDrops(id)
        ]);
        if (!data.relic) return res.status(404).json({ error: 'Relic not found' });
        res.json({ ...data, relic: { ...data.relic, drops } });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid relic ID' });
        const [relicRows, drops] = await Promise.all([getRelic(id), getRelicDrops(id)]);
        const relic = relicRows[0];
        if (!relic) return res.status(404).json({ error: 'Relic not found' });
        res.json({ ...relic, drops });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

export default router;