import express from 'express';
import { getAllPrimeParts, getPrimePart } from '../database/database';

const router = express.Router();

const primePartsDebugger = require('debug')('app:primePartsEndpoint');

router.get('/', async (req, res) => {
    try {
        const primeParts = await getAllPrimeParts();
        res.json({ results: primeParts });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:setName/:partName', async (req, res) => {
    try {
        const { setName, partName } = req.params;
        const part = await getPrimePart(setName, partName);
        if (!part) return res.status(404).json({ error: 'Prime part not found' });
        res.json(part);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

export default router;