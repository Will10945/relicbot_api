import express from 'express';
const router = express.Router();

router.get('/', (req, res) => {
    res.render('index', {title: 'Relicbot API', message: `${router.stack}`});
});

export default router;
