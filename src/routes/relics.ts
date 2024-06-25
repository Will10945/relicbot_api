import express from 'express';
import { getAllRelics, getRelic } from '../database/database';

const router = express.Router();

const relicDebugger = require('debug')('app:relicsEndpoint');

router.get('/', async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
    try {
        const relics = await getAllRelics();
        res.json({ results: relics });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
});

router.get('/:id', async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
    try {
        const relic = (await getRelic(parseInt(req.params.id)))[0];
        res.json(relic);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
})

export default router;