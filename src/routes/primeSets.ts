import express from 'express';
import { getPrimeSetsWithParts, getPrimeSetByName } from '../database/database';

const router = express.Router();

const primeSetsDebugger = require('debug')('app:primeSetsEndpoint');

router.get('/', async (req, res) => {
    try {
        const sets = await getPrimeSetsWithParts();
        const results = sets.map((s) => ({
            primeSet: s.PrimeSet,
            price: s.Price,
            partsTotalPrice: s.PartsTotalPrice ?? undefined,
            ducats: s.Ducats,
            category: s.Category,
            vaulted: Boolean(s.Vaulted),
            parts: s.parts
        }));
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:setName', async (req, res) => {
    try {
        const setName = req.params.setName;
        const set = await getPrimeSetByName(setName);
        if (!set) return res.status(404).json({ error: 'Prime set not found' });
        res.json({
            primeSet: set.PrimeSet,
            price: set.Price,
            partsTotalPrice: set.PartsTotalPrice ?? undefined,
            ducats: set.Ducats,
            category: set.Category,
            vaulted: Boolean(set.Vaulted),
            parts: set.parts
        });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

export default router;