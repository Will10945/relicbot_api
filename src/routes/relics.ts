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