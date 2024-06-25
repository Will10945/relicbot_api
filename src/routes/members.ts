import Joi from 'joi';
import express from 'express';
import IMemberRow from '../models/db.member';
import { getAllMembers, getMemberById, ModifyQuery } from '../database/database';

const router = express.Router();

router.get('/', async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
    try {
        const members = await getAllMembers();
        res.json({ results: members });
    } catch (error) {
        res.status(500).json({ error: 'Internal server error' })
    }
});

router.get('/:id', async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
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
