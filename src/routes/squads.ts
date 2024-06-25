import express from 'express';
import { getActiveSquads, getAllSquads, getSquadById } from '../database/database';
import ISquadRow, { ISquadPostRow, ISquadRefinementRow, ISquadRelicRow, ISquadUserRow, Squad } from '../models/db.squads';
import { serialize } from 'v8';
import zlib from 'zlib';

const router = express.Router();

const squadDebugger = require('debug')('app:squadsEndpoint');

var ACTIVE_SQUADS: Squad[] = [];

async function fetchSquads() {
    const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getActiveSquads();
    const data = await mergeSquadQueryResults(squads, squadUsers, squadRelics, squadRefinements, squadPosts);
    return data;
}

async function updateSquads() {
    try {
        ACTIVE_SQUADS = await fetchSquads();
        console.info('Updated squads global successfully');
    } catch (error) {
        console.error('Error updating squads global', error);
    } finally {
        setTimeout(updateSquads, 0);
    }
}

updateSquads();

async function mergeSquadQueryResults(
    squadResults: ISquadRow[], squadUsersResults: ISquadUserRow[], squadRelicsResults: ISquadRelicRow[], 
    squadRefinementResults: ISquadRefinementRow[], squadPostsResults: ISquadPostRow[] ): Promise<Squad[]> {

    const mergedMap: { [SquadID: string]: Squad } = {};

    squadResults.forEach(res => {

        if (!mergedMap[res.SquadID]) {
            mergedMap[res.SquadID] = {
                SquadID: res.SquadID,
                Style: res.Style,
                Era: res.Era,
                CycleRequirement: res.CycleRequirement,
                Host: res.Host,
                CurrentCount: res.CurrentCount,
                Filled: res.Filled,
                UserMsg: res.UserMsg,
                CreatedAt: res.CreatedAt,
                Active: res.Active,
                OriginatingServer: res.OriginatingServer,
                Rehost: res.Rehost,
                ClosedAt: res.ClosedAt,
                MemberIDs: {},
                RelicIDs: {},
                RefinementIDs: {
                    Oncycle: [],
                    Offcycle: []
                }
            }
        }
        
    });

    squadUsersResults.forEach(({ SquadID, MemberID, ServerID, AnonymousUsers }) => {
        if (!mergedMap[SquadID]) return;

        if (MemberID && !mergedMap[SquadID].MemberIDs[MemberID]) {
            mergedMap[SquadID].MemberIDs[MemberID] = {
                ServerID,
                AnonymousUsers
            }
        }
    });
    squadDebugger('SquadUsers slotted...')

    squadRelicsResults.forEach(({ SquadID, RelicID, Offcycle }) => {
        if (!mergedMap[SquadID]) return;

        if (RelicID && !mergedMap[SquadID].RelicIDs[RelicID]) {
            mergedMap[SquadID].RelicIDs[RelicID] = {
                Offcycle
            }
        }
    });
    squadDebugger('SquadRelics slotted...')

    squadPostsResults.forEach(({ SquadID, MessageID, ChannelID }) => {
        if (!mergedMap[SquadID]) return;
        if (!mergedMap[SquadID]['MessageIDs']) mergedMap[SquadID]['MessageIDs'] = {}

        if (MessageID && !mergedMap[SquadID].MessageIDs![MessageID]) {
            mergedMap[SquadID].MessageIDs![MessageID] = {
                ChannelID
            }
        }
    });
    squadDebugger('SquadPosts slotted...')
    
    squadRefinementResults.forEach(({ SquadID, RefinementID, Offcycle }) => {
        if (!mergedMap[SquadID]) return;

        if (!Offcycle && !mergedMap[SquadID].RefinementIDs.Oncycle.includes(RefinementID)) 
            mergedMap[SquadID].RefinementIDs.Oncycle.push(RefinementID);
        else if (Offcycle && !mergedMap[SquadID].RefinementIDs.Offcycle.includes(RefinementID)) 
            mergedMap[SquadID].RefinementIDs.Offcycle.push(RefinementID);
    });
    squadDebugger('SquadRefinements slotted...')

    return Object.values(mergedMap);
}

router.get('/', async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
    try {
        const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getAllSquads();
        const squadsFormatted: Squad[] = await mergeSquadQueryResults(squads, squadUsers, squadRelics, squadRefinements, squadPosts);
        res.json({ results: squadsFormatted });
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: 'Internal server error' })
    }
});

router.get('/active', async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
    try {
        // const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getActiveSquads();
        // const squadsFormatted: Squad[] = await mergeSquadQueryResults(squads, squadUsers, squadRelics, squadRefinements, squadPosts);
        // res.json({ results: squadsFormatted });
        console.info('Squads requested ...');
        res.json({ results: ACTIVE_SQUADS });
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: 'Internal server error' })
    }
});

router.get('/:id', async (req, res) => {
    const { squad, squadUsers, squadRelics, squadRefinements, squadPosts } = await getSquadById(req.params.id);
    const squadsFormatted: Squad[] = await mergeSquadQueryResults(squad, squadUsers, squadRelics, squadRefinements, squadPosts);
    res.json(squadsFormatted);
});

router.get('/users/:memberId/:memberId2?/:memberId3?/:memberId4?', async (req, res) => {
    const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getAllSquads();
    const squadsFormatted: Squad[] = await mergeSquadQueryResults(squads, squadUsers, squadRelics, squadRefinements, squadPosts);
    const memberIds = Object.values(req.params).filter(v => v);
    const userSquads = squadsFormatted.filter((s) => {
        if (memberIds.every(e => Object.keys(s.MemberIDs).includes(e))) {
            return s;
        };
    });
    res.json(userSquads);
})

router.get('/relics/:relicId', async (req, res) => {
    const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getAllSquads();
    const squadsFormatted: Squad[] = await mergeSquadQueryResults(squads, squadUsers, squadRelics, squadRefinements, squadPosts);
    const relicIds = Object.values(req.params).filter(v => v);
    const relicSquads = squadsFormatted.filter((s) => {
        if (relicIds.every(e => Object.keys(s.RelicIDs).includes(e))) {
            squadDebugger(Object.keys(s.RelicIDs));
            return s;
        };
    });
    res.json(relicSquads);
})


export default router;