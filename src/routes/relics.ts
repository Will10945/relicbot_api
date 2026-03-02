import express from 'express';
import { getAllRelics, getRelic, getRelicProfileData, getProfileOrderBy, parseFilledOnlyParam, parseDateRange, normalizeFromTo } from '../database/database';

const router = express.Router();

const relicDebugger = require('debug')('app:relicsEndpoint');

router.get('/', async (req, res) => {
    try {
        const relics = await getAllRelics();
        res.json({ results: relics });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
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
        const data = await getRelicProfileData(id, sortBy, filledOnly, dateRange);
        if (!data.relic) return res.status(404).json({ error: 'Relic not found' });
        res.json(data);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:id', async (req, res) => {
    try {
        const relic = (await getRelic(parseInt(req.params.id)))[0];
        res.json(relic);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
})

export default router;