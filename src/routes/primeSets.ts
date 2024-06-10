import express from 'express';
import { getAllPrimeSets } from '../database/database';

const router = express.Router();

const primeSetsDebugger = require('debug')('app:primeSetsEndpoint');

router.get('/', async (req, res) => {
    try {
        const primeSets = await getAllPrimeSets();
        res.json(primeSets);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
});

export default router;