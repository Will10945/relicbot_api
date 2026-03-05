import Joi from 'joi';
import express from 'express';
import IMemberRow from '../entities/db.member';
import { getAllMembers, getMemberById, getMemberProfileData, getProfileOrderBy, parseFilledOnlyParam, parseDateRange, normalizeFromTo, ModifyQuery, getMemberReputationPerDay, getMemberReputationPerDayOrderBy } from '../database/database';

const router = express.Router();

router.get('/', async (req, res) => {
    try {
        const members = await getAllMembers();
        res.json({ results: members });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
});

/** Parse member IDs from query (?ids=1,2,3 or ?ids=1&ids=2) or body (POST { ids: [1,2,3] }). Returns null if invalid. */
function parseMemberIdsFromRequest(req: express.Request): number[] | null {
    const raw = req.body?.ids ?? req.query.ids;
    if (raw === undefined || raw === null) return null;
    const arr = Array.isArray(raw) ? raw : (typeof raw === 'string' ? raw.split(',').map((s) => s.trim()) : [raw]);
    const ids: number[] = [];
    for (const v of arr) {
        const n = typeof v === 'string' ? parseInt(v, 10) : Number(v);
        if (!Number.isInteger(n) || n < 1) return null;
        ids.push(n);
    }
    return ids.length === 0 ? null : [...new Set(ids)];
}

router.get('/profiles', async (req, res) => {
    try {
        const ids = parseMemberIdsFromRequest(req);
        if (!ids) return res.status(400).json({ error: 'Invalid or missing ids; use query ?ids=1,2,3' });
        const sortBy = getProfileOrderBy((req.query.sort as string) ?? '');
        const filledOnly = parseFilledOnlyParam(req.query.all);
        const { from, to } = normalizeFromTo(req.query as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const profilesData = await Promise.all(ids.map((id) => getMemberProfileData(id, sortBy, filledOnly, dateRange)));
        const results = profilesData.filter((data) => data.member != null);
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.post('/profiles', async (req, res) => {
    try {
        const ids = parseMemberIdsFromRequest(req);
        if (!ids) return res.status(400).json({ error: 'Invalid or missing ids; send JSON body { "ids": [1, 2, 3] }' });
        const sortBy = getProfileOrderBy((req.query.sort as string) ?? req.body?.sort ?? '');
        const filledOnly = parseFilledOnlyParam(req.query.all ?? req.body?.all);
        const { from, to } = normalizeFromTo({ ...req.query, ...req.body } as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const profilesData = await Promise.all(ids.map((id) => getMemberProfileData(id, sortBy, filledOnly, dateRange)));
        const results = profilesData.filter((data) => data.member != null);
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:id/profile', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid member ID' });
        const sortBy = getProfileOrderBy((req.query.sort as string) ?? '');
        const filledOnly = parseFilledOnlyParam(req.query.all);
        const { from, to } = normalizeFromTo(req.query as Record<string, unknown>);
        const dateRange = parseDateRange(from, to);
        const data = await getMemberProfileData(id, sortBy, filledOnly, dateRange);
        if (!data.member) return res.status(404).json({ error: 'Member not found' });
        res.json(data);
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:id/reputation-per-day', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid member ID' });
        const member = (await getMemberById(id))[0];
        if (!member) return res.status(404).json({ error: 'Member not found' });
        const sortBy = getMemberReputationPerDayOrderBy((req.query.sort as string) ?? 'date');
        const results = await getMemberReputationPerDay(id, sortBy);
        res.json({ results });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/:id', async (req, res) => {
    const member = (await getMemberById(parseInt(req.params.id)))[0];
    if (!member) return res.status(404).send('The member with the given id does not exist.');
    res.send(member);
});

router.delete('/:id', async (req, res) => {
    const member = await getMemberById(parseInt(req.params.id));
    if (!member) return res.status(404).send('User with given ID not found in database.');

    await ModifyQuery(`DELETE FROM Members WHERE MemberID = ${member[0].MemberID}`);

    res.send(member);

})

router.post('/', async (req, res) => {

    const { value, error } = validateMember(req.body);

    if (error) return res.status(400).send(error.details[0].message);

    const member = {
        MemberName: req.body.MemberName,
        DiscordID: req.body.DiscordID,
    }

    res.send(`${value.MemberName} successfully added to the database with discord id: ${value.DiscordID}`)

    return;

    await ModifyQuery(`INSERT INTO Members (MemberName, DiscordID) VALUES ("${member.MemberName}", ${member.DiscordID})`);

    const newMember = (await getAllMembers()).find(m => m.MemberName === req.body.MemberName);
    if (!newMember) res.status(400).send('User unsuccessfully added to the database.')
    res.send(newMember);
});

router.put('/:id', async (req, res) => {
    // Look up member
    // If member does not exist, return 404
    const member = await getMemberById(parseInt(req.params.id));
    if (!member) return res.status(404).send('The member with the given id does not exist.');

    // Validate
    // In invalid, return 400
    
    const { value, error } = validateMember(req.body);
    if (error) return res.status(400).send(error.details[0].message);

    // Update member
    const updatedMember = { ...member, ...value };

    // Return updated member
    res.send(updatedMember);

});

function validateMember(member: IMemberRow) {
    const schema = Joi.object({
        MemberID: Joi.number(),
        MemberName: Joi.string().min(3).required(),
        DiscordID: Joi.number().required(),
        AllowMerging: Joi.number(),
        HostLimit: Joi.number().positive(),
        CycleDefault: Joi.number().positive(),
        Muted: Joi.number().positive(),
        Badge: Joi.number(),
        SquadChatTime: Joi.number(),
        Admin: Joi.bool(),
    });

    return schema.validate(member);
}

export default router;
