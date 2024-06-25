import express from 'express';
import { getAllPrimeParts } from '../database/database';

const router = express.Router();

const primeSetsDebugger = require('debug')('app:primePartsEndpoint');

router.get('/', async (req, res) => {
    try {
        const primeParts = await getAllPrimeParts();
        res.json({ results: primeParts });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
});

export default router;