import express from 'express';
import { getAllRelics } from '../database/database';

const router = express.Router();

const relicDebugger = require('debug')('app:relicsEndpoint');

router.get('/', async (req, res) => {
    try {
        const relics = await getAllRelics();
        res.json(relics);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
});

export default router;