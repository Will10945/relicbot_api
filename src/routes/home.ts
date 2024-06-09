import express from 'express';
const router = express.Router();

router.get('/', (req, res) => {
    res.render('index', {title: 'Relicbot API', message: 'Relicbot API but not the title this time'});
});

export default router;
