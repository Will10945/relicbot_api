import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import {
    getActiveSquads,
    getAllSquads,
    getSquadById,
    getSquadsByIds,
    getActiveSquadIdsByMemberId,
    getLeaveContextBatch,
    executeLeaveBulk,
    type LeaveBulkOps,
    createSquadsWithDetailsBatch,
    SquadInsertData,
    getSquadUser,
    addSquadMember,
    addSquadGuest,
    updateReputationOnSquadClosed,
    getSquadCountsPerDay,
    getSquadCountsPerDayOrderBy,
} from '../database/database';
import ISquadRow, { ISquadPostRow, ISquadRefinementRow, ISquadRelicRow, ISquadUserRow, Squad } from '../entities/db.squads';
import _ from 'underscore';
import Joi from 'joi';
import TABLES from '../entities/constants';

const router = express.Router();

const squadDebugger = require('debug')('app:squadsEndpoint');

interface SquadMemberInput {
    memberId: number;
    serverId?: number;
    anonymousUsers?: number;
}

interface SquadRelicInput {
    relicId: number;
    offcycle?: boolean;
}

interface SquadRefinementInput {
    refinementId: number;
    offcycle?: boolean;
}

interface SquadCreateRequest {
    /** Host member; if members is omitted, squad is created with host as sole member. */
    hostMemberId: number;
    /** At least one relic required. Format is [relic] [style] [refinement]; style and refinement can be filled automatically. */
    relics: SquadRelicInput[];
    style?: string;
    era?: string;
    cycleRequirement?: number;
    /** Optional; legacy from Discord bot. Defaults to 0. */
    originatingServerId?: number;
    userMsg?: string;
    /** Optional; defaults to host as sole member. */
    members?: SquadMemberInput[];
    refinements?: SquadRefinementInput[];
}

function parseNumberArrayParam(value: unknown): number[] | undefined {
    if (!value) return undefined;
    const str = String(value);
    const parts = str.split(',');
    const nums = parts
        .map(p => parseInt(p.trim(), 10))
        .filter(n => !Number.isNaN(n));
    return nums.length ? nums : undefined;
}

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
                CloseReason: res.CloseReason,
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

async function getSquadDto(squadId: string): Promise<Squad | null> {
    const { squad, squadUsers, squadRelics, squadRefinements, squadPosts } = await getSquadById(squadId);
    const merged = await mergeSquadQueryResults(squad, squadUsers, squadRelics, squadRefinements, squadPosts);
    return merged[0] ?? null;
}

/** Fetch multiple squads as DTOs in one batch (5 queries total), ordered to match squadIds. */
async function batchGetSquadDtos(squadIds: string[]): Promise<Squad[]> {
    if (squadIds.length === 0) return [];
    const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getSquadsByIds(squadIds);
    const merged = await mergeSquadQueryResults(squads, squadUsers, squadRelics, squadRefinements, squadPosts);
    const byId = new Map(merged.map((s) => [s.SquadID, s]));
    return squadIds.map((id) => byId.get(id)).filter((s): s is Squad => s != null);
}

router.get('/', async (req, res) => {
    try {
        const {
            status = 'all',
            memberIds,
            relicIds,
            refinementIds,
            era,
            style,
            hostMemberId,
            originatingServerId,
            filled
        } = req.query as {
            status?: string;
            memberIds?: string;
            relicIds?: string;
            refinementIds?: string;
            era?: string;
            style?: string;
            hostMemberId?: string;
            originatingServerId?: string;
            filled?: string;
        };

        const parsedMemberIds = parseNumberArrayParam(memberIds);
        const parsedRelicIds = parseNumberArrayParam(relicIds);
        const parsedRefinementIds = parseNumberArrayParam(refinementIds);
        const parsedHostMemberId = hostMemberId != null && hostMemberId !== '' ? parseInt(String(hostMemberId), 10) : undefined;
        const parsedOriginatingServerId = originatingServerId != null && originatingServerId !== '' ? parseInt(String(originatingServerId), 10) : undefined;
        const parsedFilled = filled === '1' || filled === '0' ? parseInt(filled, 10) : undefined;

        const hasFilters =
            !!parsedMemberIds?.length ||
            !!parsedRelicIds?.length ||
            !!parsedRefinementIds?.length ||
            !!era ||
            !!style ||
            parsedHostMemberId !== undefined ||
            parsedOriginatingServerId !== undefined ||
            parsedFilled !== undefined;

        const fetchFn = status === 'active' ? getActiveSquads : getAllSquads;
        const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await fetchFn();
        const squadsFormatted: Squad[] = await mergeSquadQueryResults(
            squads,
            squadUsers,
            squadRelics,
            squadRefinements,
            squadPosts
        );

        let filtered = squadsFormatted;

        if (parsedMemberIds?.length) {
            filtered = filtered.filter((s) =>
                parsedMemberIds.every((id) =>
                    Object.prototype.hasOwnProperty.call(s.MemberIDs, id)
                )
            );
        }

        if (parsedRelicIds?.length) {
            filtered = filtered.filter((s) =>
                parsedRelicIds.every((id) =>
                    Object.prototype.hasOwnProperty.call(s.RelicIDs, id)
                )
            );
        }

        if (era) {
            filtered = filtered.filter((s) => s.Era === era);
        }

        if (style) {
            filtered = filtered.filter((s) => s.Style === style);
        }

        if (parsedRefinementIds?.length) {
            filtered = filtered.filter((s) => {
                const onAndOff = [...s.RefinementIDs.Oncycle, ...s.RefinementIDs.Offcycle];
                return parsedRefinementIds.some((id) => onAndOff.includes(id));
            });
        }

        if (parsedHostMemberId !== undefined) {
            filtered = filtered.filter((s) => s.Host === parsedHostMemberId);
        }

        if (parsedOriginatingServerId !== undefined) {
            filtered = filtered.filter((s) => s.OriginatingServer === parsedOriginatingServerId);
        }

        if (parsedFilled !== undefined) {
            filtered = filtered.filter((s) => s.Filled === parsedFilled);
        }

        res.json({ results: filtered });
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

router.get('/active', async (req, res) => {
    try {
        const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getActiveSquads();
        const results = await mergeSquadQueryResults(
            squads,
            squadUsers,
            squadRelics,
            squadRefinements,
            squadPosts
        );
        res.json({ results });
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** GET /api/squads/counts-per-day — count of closed squads per day (total, filled, unfilled). Includes every day in range (zeros for no data). ?sort=date|filled|total|unfilled (default: date ascending; others by value descending). */
router.get('/counts-per-day', async (req, res) => {
    try {
        const sortBy = getSquadCountsPerDayOrderBy((req.query.sort as string) ?? '');
        const results = await getSquadCountsPerDay(sortBy);
        res.json({ results });
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** Join squads (1 or more). Body: { memberId, serverId?, squadIds: string[] }.
 *  Adding a guest: when the host joins again (member already in squad), only when squadIds.length === 1, we add one guest instead of inserting a duplicate row. */
router.post('/join', async (req, res) => {
    const schema = Joi.object({
        memberId: Joi.number().integer().required(),
        serverId: Joi.number().integer().min(0).optional().default(0),
        squadIds: Joi.array().items(Joi.string().uuid()).min(1).required()
    });
    const { error, value } = schema.validate(req.body);
    if (error) {
        return res.status(400).json({ errors: error.details.map((d) => d.message) });
    }
    const serverId = value.serverId ?? 0;
    const singleSquad = value.squadIds.length === 1;
    const successSquadIds: string[] = [];
    const errors: { index: number; message: string }[] = [];
    for (let i = 0; i < value.squadIds.length; i++) {
        const squadId = value.squadIds[i];
        try {
            const existing = await getSquadUser(squadId, value.memberId);
            if (existing) {
                if (singleSquad) {
                    await addSquadGuest(squadId, value.memberId);
                    successSquadIds.push(squadId);
                } else {
                    errors.push({ index: i, message: 'Already in squad; guest add only when joining one squad' });
                }
            } else {
                await addSquadMember(squadId, value.memberId, serverId);
                successSquadIds.push(squadId);
            }
        } catch (err) {
            errors.push({ index: i, message: err instanceof Error ? err.message : String(err) });
        }
    }
    const completed = await batchGetSquadDtos(successSquadIds);
    res.status(completed.length > 0 ? 200 : 422).json({
        completed,
        ...(errors.length > 0 && { errors })
    });
});

/** Leave squads. Body: { memberId, squadIds?: string[] }.
 *  If squadIds is omitted or empty, the member is removed from every active squad they are in (active squads created in the last 24 hours only).
 *  Removing a guest: when leaving one squad only and the member has guests (AnonymousUsers > 0), we remove one guest.
 *  Bulk: when leaving multiple squads, the member (and all their guests) are removed from each squad. */
router.post('/leave', async (req, res) => {
    const schema = Joi.object({
        memberId: Joi.number().integer().required(),
        squadIds: Joi.array().items(Joi.string().uuid()).optional()
    });
    const { error, value } = schema.validate(req.body);
    if (error) {
        return res.status(400).json({ errors: error.details.map((d) => d.message) });
    }
    let squadIdsToLeave: string[] = value.squadIds ?? [];
    if (squadIdsToLeave.length === 0) {
        squadIdsToLeave = await getActiveSquadIdsByMemberId(value.memberId);
    }
    if (squadIdsToLeave.length === 0) {
        return res.status(200).json({ completed: [], message: 'Member is not in any active squads' });
    }
    const singleSquad = squadIdsToLeave.length === 1;
    const { userRowsBySquadId, squadHostAndUsers } = await getLeaveContextBatch(value.memberId, squadIdsToLeave);

    const bulkOps: LeaveBulkOps = {
        guestRemovals: [],
        nonHostLeaves: [],
        hostRehosts: [],
        hostCloses: []
    };
    const successSquadIds: string[] = [];
    const errors: { index: number; message: string }[] = [];

    for (let i = 0; i < squadIdsToLeave.length; i++) {
        const squadId = squadIdsToLeave[i];
        const userRow = userRowsBySquadId.get(squadId);
        if (!userRow) {
            errors.push({ index: i, message: 'Member not in squad' });
            continue;
        }
        const hostAndUsers = squadHostAndUsers.get(squadId);
        if (!hostAndUsers) {
            errors.push({ index: i, message: 'Squad not found' });
            continue;
        }
        const anonymousUsers = userRow.AnonymousUsers ?? 0;
        const isHost = hostAndUsers.host === value.memberId;
        const otherRows = hostAndUsers.users.filter((r) => r.MemberID !== value.memberId);

        if (singleSquad && anonymousUsers > 0) {
            bulkOps.guestRemovals.push({ squadId, memberId: value.memberId });
        } else {
            if (!isHost) {
                bulkOps.nonHostLeaves.push({ squadId, memberId: value.memberId, anonymousUsers });
            } else {
                if (otherRows.length >= 1) {
                    bulkOps.hostRehosts.push({
                        squadId,
                        memberId: value.memberId,
                        anonymousUsers,
                        newHostMemberId: otherRows[0].MemberID
                    });
                } else {
                    bulkOps.hostCloses.push({
                        squadId,
                        memberId: value.memberId,
                        anonymousUsers,
                        closedAt: Math.floor(Date.now() / 1000),
                        filled: 0,
                        closeReason: 'host_left'
                    });
                }
            }
        }
        successSquadIds.push(squadId);
    }

    try {
        await executeLeaveBulk(bulkOps);
        for (const op of bulkOps.hostCloses) {
            updateReputationOnSquadClosed(op.squadId).catch((err) =>
                squadDebugger('Reputation update failed for squad %s: %s', op.squadId, err)
            );
        }
    } catch (err) {
        errors.push({ index: -1, message: err instanceof Error ? err.message : String(err) });
        successSquadIds.length = 0;
    }
    const completed = await batchGetSquadDtos(successSquadIds);
    res.status(200).json({
        completed,
        ...(errors.length > 0 && { errors })
    });
});

router.get('/:id', async (req, res) => {
    try {
        const { squad, squadUsers, squadRelics, squadRefinements, squadPosts } = await getSquadById(req.params.id);
        const squadsFormatted: Squad[] = await mergeSquadQueryResults(
            squad,
            squadUsers,
            squadRelics,
            squadRefinements,
            squadPosts
        );

        if (!squadsFormatted.length) {
            return res.status(404).json({ error: 'Squad not found' });
        }

        res.json(squadsFormatted[0]);
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

/** Signature for duplicate detection: relics + style + refinements + cycle requirement. */
function createItemSignature(item: SquadCreateRequest): string {
    const relicPart = item.relics
        .map((r) => `${r.relicId}:${r.offcycle ? 1 : 0}`)
        .sort()
        .join(',');
    const refPart = (item.refinements ?? [])
        .map((r) => `${r.refinementId}:${r.offcycle ? 1 : 0}`)
        .sort()
        .join(',');
    const cycle = item.cycleRequirement !== undefined && item.cycleRequirement !== null ? String(item.cycleRequirement) : '';
    return `${item.era ?? ''}|${item.style ?? ''}|${cycle}|${relicPart}|${refPart}`;
}

function squadSignature(s: Squad): string {
    const relicPart = Object.entries(s.RelicIDs ?? {})
        .map(([id, o]) => `${id}:${o.Offcycle ?? 0}`)
        .sort()
        .join(',');
    const refParts = [
        ...(s.RefinementIDs?.Oncycle ?? []).map((id) => `${id}:0`),
        ...(s.RefinementIDs?.Offcycle ?? []).map((id) => `${id}:1`)
    ].sort();
    const refPart = refParts.join(',');
    const cycle = s.CycleRequirement !== undefined && s.CycleRequirement !== null ? String(s.CycleRequirement) : '';
    return `${s.Era ?? ''}|${s.Style ?? ''}|${cycle}|${relicPart}|${refPart}`;
}

/** Create squads. Body: list of squad creation objects (min 1). Equivalency: relics + style + refinements + cycle requirement. Duplicates within the same request are skipped (only one create/join per signature). Against existing: if user is host of matching squad → skip; if user not in matching squad → join it. */
router.post('/create', async (req, res) => {
    const squadCreateSchema = Joi.object({
        hostMemberId: Joi.number().integer().required(),
        relics: Joi.array()
            .items(
                Joi.object({
                    relicId: Joi.number().integer().required(),
                    offcycle: Joi.boolean().optional()
                })
            )
            .min(1)
            .required(),
        style: Joi.string().pattern(/^[1-4]b[1-4]$/).optional(),
        era: Joi.string().valid('Lith', 'Meso', 'Neo', 'Axi').optional(),
        cycleRequirement: Joi.number().integer().min(0).optional(),
        originatingServerId: Joi.number().integer().min(0).optional().default(0),
        userMsg: Joi.string().max(1000).allow('', null).optional(),
        members: Joi.array()
            .items(
                Joi.object({
                    memberId: Joi.number().integer().required(),
                    serverId: Joi.number().integer().min(0).optional().default(0),
                    anonymousUsers: Joi.number().integer().min(0).default(0)
                })
            )
            .optional(),
        refinements: Joi.array()
            .items(
                Joi.object({
                    refinementId: Joi.number().integer().required(),
                    offcycle: Joi.boolean().optional()
                })
            )
            .optional()
    });

    try {
        const payload = req.body;
        if (!Array.isArray(payload) || payload.length === 0) {
            return res.status(400).json({
                errors: ['Body must be a non-empty array of squad creation objects']
            });
        }

        const { error, value } = Joi.array()
            .items(squadCreateSchema)
            .min(1)
            .validate(payload, { abortEarly: false });

        if (error) {
            return res.status(400).json({
                errors: error.details.map((d) => d.message)
            });
        }

        const createItems = value as SquadCreateRequest[];
        const serverDefault = 0;
        const now = Math.floor(Date.now() / 1000);

        const { squads, squadUsers, squadRelics, squadRefinements, squadPosts } = await getActiveSquads();
        const activeSquadsList = await mergeSquadQueryResults(
            squads,
            squadUsers,
            squadRelics,
            squadRefinements,
            squadPosts
        );
        const signatureToSquad = new Map<string, Squad>();
        for (const s of activeSquadsList) {
            const sig = squadSignature(s);
            if (!signatureToSquad.has(sig)) {
                signatureToSquad.set(sig, s);
            }
        }

        const toInsertList: SquadInsertData[] = [];
        const createdSquadIds: string[] = [];
        const toJoinList: { squadId: string; memberId: number; serverId: number }[] = [];
        const joinedSquadIds = new Set<string>();
        const processedSignatures = new Set<string>();

        for (const sq of createItems) {
            const sig = createItemSignature(sq);
            if (processedSignatures.has(sig)) {
                continue;
            }
            const existing = signatureToSquad.get(sig);
            const hostMemberId = sq.hostMemberId;
            const serverId = sq.originatingServerId ?? serverDefault;

            if (existing) {
                if (existing.Host === hostMemberId) {
                    processedSignatures.add(sig);
                    continue;
                }
                if (Object.prototype.hasOwnProperty.call(existing.MemberIDs ?? {}, hostMemberId)) {
                    processedSignatures.add(sig);
                    continue;
                }
                toJoinList.push({ squadId: existing.SquadID, memberId: hostMemberId, serverId });
                joinedSquadIds.add(existing.SquadID);
                processedSignatures.add(sig);
                continue;
            }

            const originatingServerId = sq.originatingServerId ?? serverDefault;
            const members =
                sq.members && sq.members.length > 0
                    ? sq.members
                    : [
                          {
                              memberId: sq.hostMemberId,
                              serverId: sq.originatingServerId ?? serverDefault,
                              anonymousUsers: 0
                          }
                      ];
            const totalAnonymous = members.reduce(
                (acc, m) => acc + (m.anonymousUsers ?? 0),
                0
            );
            const currentCount = members.length + totalAnonymous;
            const squadId = uuidv4();
            createdSquadIds.push(squadId);
            processedSignatures.add(sig);
            toInsertList.push({
                squad: {
                    SquadID: squadId,
                    Style: sq.style,
                    Era: sq.era,
                    CycleRequirement: sq.cycleRequirement,
                    Host: sq.hostMemberId,
                    CurrentCount: currentCount,
                    Filled: 0,
                    UserMsg: sq.userMsg,
                    CreatedAt: now,
                    Active: 1,
                    OriginatingServer: originatingServerId,
                    Rehost: 0,
                    ClosedAt: null
                },
                members: members.map((m) => ({
                    MemberID: m.memberId,
                    ServerID: m.serverId ?? serverDefault,
                    AnonymousUsers: m.anonymousUsers ?? 0
                })),
                relics: sq.relics.map((r) => ({
                    RelicID: r.relicId,
                    Offcycle: r.offcycle ? 1 : 0
                })),
                refinements: (sq.refinements ?? []).map((r) => ({
                    RefinementID: r.refinementId,
                    Offcycle: r.offcycle ? 1 : 0
                }))
            });
        }

        if (toInsertList.length > 0) {
            await createSquadsWithDetailsBatch(toInsertList);
        }

        const joinKeys = new Set<string>();
        for (const j of toJoinList) {
            const key = `${j.squadId}:${j.memberId}`;
            if (joinKeys.has(key)) continue;
            joinKeys.add(key);
            await addSquadMember(j.squadId, j.memberId, j.serverId);
        }

        const allSquadIds = [...createdSquadIds, ...joinedSquadIds];
        const completed = allSquadIds.length > 0 ? await batchGetSquadDtos([...allSquadIds]) : [];
        res.status(completed.length > 0 ? 201 : 200).json({ completed });
    } catch (err) {
        console.log(err);
        res.status(500).json({
            error: err instanceof Error ? err.message : 'Internal server error'
        });
    }
});

export default router;