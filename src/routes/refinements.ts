import express from 'express';
import { getAllPrimeSets, getAllRefinements } from '../database/database';

const router = express.Router();

const refinementsDebugger = require('debug')('app:refinementsEndpoint');

router.get('/', async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
    try {
        const refinements = await getAllRefinements();
        res.json({ results: refinements });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
});

export default router;