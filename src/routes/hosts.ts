import express from 'express';
import { getHostProfileData, getAllHostsProfileData, getHostProfileOrderBy, parseFilledOnlyParam, parseDateRange, normalizeFromTo } from '../database/database';

const router = express.Router();

router.get('/', async (req, res) => {
    try {
        const sortParam = (req.query.sort as string) ?? '';
        const sortBy = getHostProfileOrderBy(sortParam);
        const showAll = !parseFilledOnlyParam(req.query.all) || sortParam.toLowerCase() === 'all';
        const filledOnly = !showAll;
        const { from, to } = normalizeFromTo(req.query as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const data = await getAllHostsProfileData(sortBy, filledOnly, dateRange);
        res.json({ results: data });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid host ID' });
        const filledOnly = parseFilledOnlyParam(req.query.all);
        const { from, to } = normalizeFromTo(req.query as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const data = await getHostProfileData(id, filledOnly, dateRange);
        if (!data.host) return res.status(404).json({ error: 'Host not found' });
        res.json(data);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

export default router;
