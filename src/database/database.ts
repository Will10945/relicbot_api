import mysql, { ResultSetHeader } from 'mysql2/promise';
import dotenv from 'dotenv';
import IMemberRow from '../entities/db.member';
import ISquadRow, { ISquadPostRow, ISquadUserRow, ISquadRelicRow, ISquadRefinementRow } from '../entities/db.squads';
import IRelicRow from '../entities/db.relics';
import IPrimeSetRow from '../entities/db.primeSets';
import IPrimePartRow from '../entities/db.primeParts';
import IRefinementRow from '../entities/db.refinement';
import TABLES from '../entities/constants';

dotenv.config();

const dbDebugger = require('debug')('app:db');

const pool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE
});
dbDebugger('Connected to the database...');


export async function SelectQuery<T>(queryString: string, params?: (number | string)[]): Promise<T[]> {
    const [results] = await pool.execute(queryString, params);
    dbDebugger(`SelectQuery: ${queryString} using the params: ${params}`);
    return results as T[];
}

export async function ModifyQuery(queryString: string, params?: (number | string)[]): Promise<ResultSetHeader> {
    const [results] = await pool.execute(queryString, params);
    dbDebugger(`ModifyQuery: ${queryString} using the params: ${params}`);
    return results as ResultSetHeader;
}

export async function getAllMembers() {
    return await SelectQuery<IMemberRow>(`SELECT * FROM ${TABLES.MEMBERS}`);
}

export async function getMemberById(id: number) {
    return await SelectQuery<IMemberRow>(`SELECT * FROM ${TABLES.MEMBERS} WHERE MemberID = ?`, [id]);
}

export async function getMemberByName(name: string|number) {
    return await SelectQuery<IMemberRow>(`SELECT * FROM ${TABLES.MEMBERS} WHERE MemberName = ?`, [name]);
}

export async function getAllSquadsWithMessages() {
    var squads: ISquadRow[] = [];
    var offset: number = 0;
    const limit: number = 100000;
    while (true){
        const _results = await SelectQuery<ISquadRow>(
            `SELECT * FROM ${TABLES.SQUADS} 
            LIMIT ${limit} OFFSET ${offset};`
        );
        squads = squads.concat(_results);
        if (_results.length == 0) break;
        offset += limit;
    }
    const squadUsers = await getAllSquadUsers();
    const squadRelics = await getAllSquadRelics();
    const squadRefinements = await getAllSquadRefinements();
    const squadPosts = await getAllSquadPosts();
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getAllSquads() {
    var squads: ISquadRow[] = [];
    var offset: number = 0;
    const limit: number = 100000;
    while (true){
        const _results = await SelectQuery<ISquadRow>(
            `SELECT * FROM ${TABLES.SQUADS} 
            LIMIT ${limit} OFFSET ${offset};`
        );
        squads = squads.concat(_results);
        if (_results.length == 0) break;
        offset += limit;
    }
    const squadUsers = await getAllSquadUsers();
    const squadRelics = await getAllSquadRelics();
    const squadRefinements = await getAllSquadRefinements();
    const squadPosts: ISquadPostRow[] = [];
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

/** Cutoff: only squads created in the last 24 hours. CreatedAt is stored in milliseconds.
 *  For best performance, add an index on squads: (Active, CreatedAt). */
const ACTIVE_SQUADS_CREATED_SINCE_MS = 24 * 60 * 60 * 1000;

export async function getActiveSquads() {
    const cutoff = Date.now() - ACTIVE_SQUADS_CREATED_SINCE_MS;
    const squads = await SelectQuery<ISquadRow>(
        `SELECT * FROM ${TABLES.SQUADS} WHERE Active = 1 AND CreatedAt >= ? ORDER BY CreatedAt DESC`,
        [cutoff]
    );
    if (squads.length === 0) {
        return { squads, squadUsers: [], squadRelics: [], squadRefinements: [], squadPosts: [] };
    }
    const squadIds = squads.map((s) => s.SquadID);
    const placeholders = squadIds.map(() => '?').join(',');
    const [squadUsers, squadRelics, squadRefinements] = await Promise.all([
        SelectQuery<ISquadUserRow>(`SELECT * FROM ${TABLES.SQUADUSERS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadRelicRow>(`SELECT * FROM ${TABLES.SQUADRELICS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadRefinementRow>(`SELECT * FROM ${TABLES.SQUADREFINEMENT} WHERE SquadID IN (${placeholders})`, squadIds)
    ]);
    const squadPosts: ISquadPostRow[] = [];
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getSquadById(id: string) {
    const squad = await SelectQuery<ISquadRow>(`SELECT * FROM ${TABLES.SQUADS} WHERE SquadID = ?;`, [id]);
    const squadUsers = await SelectQuery<ISquadUserRow>(`SELECT * FROM ${TABLES.SQUADUSERS} WHERE SquadID = ?;`, [id]);
    const squadRelics = await SelectQuery<ISquadRelicRow>(`SELECT * FROM ${TABLES.SQUADRELICS} WHERE SquadID = ?;`, [id]);
    const squadRefinements = await SelectQuery<ISquadRefinementRow>(`SELECT * FROM ${TABLES.SQUADREFINEMENT} WHERE SquadID = ?;`, [id]);
    const squadPosts = await SelectQuery<ISquadPostRow>(`SELECT * FROM ${TABLES.SQUADPOSTS} WHERE SquadID = ?;`, [id]);

    return { squad, squadUsers, squadRelics, squadRefinements, squadPosts };
}

/** Fetch multiple squads and their related rows in 5 queries total. Returns same shape as getSquadById but with arrays for multiple IDs. */
export async function getSquadsByIds(squadIds: string[]) {
    if (squadIds.length === 0) {
        return { squads: [], squadUsers: [], squadRelics: [], squadRefinements: [], squadPosts: [] };
    }
    const placeholders = squadIds.map(() => '?').join(',');
    const [squads, squadUsers, squadRelics, squadRefinements, squadPosts] = await Promise.all([
        SelectQuery<ISquadRow>(`SELECT * FROM ${TABLES.SQUADS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadUserRow>(`SELECT * FROM ${TABLES.SQUADUSERS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadRelicRow>(`SELECT * FROM ${TABLES.SQUADRELICS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadRefinementRow>(`SELECT * FROM ${TABLES.SQUADREFINEMENT} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadPostRow>(`SELECT * FROM ${TABLES.SQUADPOSTS} WHERE SquadID IN (${placeholders})`, squadIds)
    ]);
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

export async function getAllSquadUsers() {
    return await SelectQuery<ISquadUserRow>(
        `SELECT * FROM ${TABLES.SQUADUSERS};`
    );
}

export async function getAllSquadRelics() {
    return await SelectQuery<ISquadRelicRow>(
        `SELECT * FROM ${TABLES.SQUADRELICS};`
    );
}

export async function getAllSquadRefinements() {
    return await SelectQuery<ISquadRefinementRow>(
        `SELECT * FROM ${TABLES.SQUADREFINEMENT};`
    );
}

export async function getAllSquadPosts() {
    return await SelectQuery<ISquadPostRow>(
        `SELECT * FROM ${TABLES.SQUADPOSTS};`
    );
}

export async function getAllRelics() {
    return await SelectQuery<IRelicRow>(
        `SELECT * FROM ${TABLES.RELICS}`
    );
}

export async function getRelic(id: number) {
    return await SelectQuery<IRelicRow>(
        `SELECT * FROM ${TABLES.RELICS} WHERE ID = ?`, [id]
    )
}

export async function getAllPrimeSets() {
    return await SelectQuery<IPrimeSetRow>(
        `SELECT * FROM ${TABLES.PRIMESETS}`
    );
}

export async function getAllPrimeParts() {
    return await SelectQuery<IPrimePartRow>(
        `SELECT * FROM ${TABLES.PRIMEPARTS}`
    );
}

export async function getAllRefinements() {
    return await SelectQuery<IRefinementRow>(
        `SELECT * FROM ${TABLES.REFINEMENT}`
    );
}

export interface SquadInsertData {
    squad: {
        SquadID: string;
        Style?: string;
        Era?: string;
        CycleRequirement?: number;
        Host: number;
        CurrentCount: number;
        Filled: number;
        UserMsg?: string;
        CreatedAt: number;
        Active: number;
        OriginatingServer: number;
        Rehost: number;
        ClosedAt: number | null;
    };
    members: {
        MemberID: number;
        ServerID: number;
        AnonymousUsers: number;
    }[];
    relics: {
        RelicID: number;
        Offcycle: number;
    }[];
    refinements: {
        RefinementID: number;
        Offcycle: number;
    }[];
}

/** Insert a single squad and its related rows in one transaction. Use for per-item processing. */
export async function createOneSquadWithDetails(squadData: SquadInsertData): Promise<void> {
    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        const sq = squadData;
        const {
            SquadID,
            Style,
            Era,
            CycleRequirement,
            Host,
            CurrentCount,
            Filled,
            UserMsg,
            CreatedAt,
            Active,
            OriginatingServer,
            Rehost,
            ClosedAt
        } = sq.squad;

        await connection.execute(
            `INSERT INTO ${TABLES.SQUADS}
            (SquadID, Style, Era, CycleRequirement, Host, CurrentCount, Filled, UserMsg, CreatedAt, Active, OriginatingServer, Rehost, ClosedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [
                SquadID,
                Style ?? null,
                Era ?? null,
                CycleRequirement ?? null,
                Host,
                CurrentCount,
                Filled,
                UserMsg ?? null,
                CreatedAt,
                Active,
                OriginatingServer,
                Rehost,
                ClosedAt ?? null
            ]
        );

        for (const member of sq.members) {
            await connection.execute(
                `INSERT INTO ${TABLES.SQUADUSERS}
                (SquadID, MemberID, ServerID, AnonymousUsers)
                VALUES (?, ?, ?, ?)`,
                [SquadID, member.MemberID, member.ServerID, member.AnonymousUsers]
            );
        }

        for (const relic of sq.relics) {
            await connection.execute(
                `INSERT INTO ${TABLES.SQUADRELICS}
                (SquadID, RelicID, Offcycle)
                VALUES (?, ?, ?)`,
                [SquadID, relic.RelicID, relic.Offcycle]
            );
        }

        for (const refinement of sq.refinements) {
            await connection.execute(
                `INSERT INTO ${TABLES.SQUADREFINEMENT}
                (SquadID, RefinementID, Offcycle)
                VALUES (?, ?, ?)`,
                [SquadID, refinement.RefinementID, refinement.Offcycle]
            );
        }

        await connection.commit();
    } catch (error) {
        await connection.rollback();
        throw error;
    } finally {
        connection.release();
    }
}

export async function createSquadsWithDetails(squadsData: SquadInsertData[]): Promise<void> {
    if (!squadsData.length) return;
    for (const sq of squadsData) {
        await createOneSquadWithDetails(sq);
    }
}

/** Insert multiple squads and their related rows in one transaction with batched INSERTs. */
export async function createSquadsWithDetailsBatch(squadsData: SquadInsertData[]): Promise<void> {
    if (!squadsData.length) return;
    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        const squadValues: (string | number | null)[] = [];
        const squadPlaceholders: string[] = [];
        for (const sq of squadsData) {
            const s = sq.squad;
            squadPlaceholders.push('(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
            squadValues.push(
                s.SquadID,
                s.Style ?? null,
                s.Era ?? null,
                s.CycleRequirement ?? null,
                s.Host,
                s.CurrentCount,
                s.Filled,
                s.UserMsg ?? null,
                s.CreatedAt,
                s.Active,
                s.OriginatingServer,
                s.Rehost,
                s.ClosedAt ?? null
            );
        }
        await connection.execute(
            `INSERT INTO ${TABLES.SQUADS}
            (SquadID, Style, Era, CycleRequirement, Host, CurrentCount, Filled, UserMsg, CreatedAt, Active, OriginatingServer, Rehost, ClosedAt)
            VALUES ${squadPlaceholders.join(', ')}`,
            squadValues
        );
        const memberRows: (string | number)[] = [];
        const relicRows: (string | number)[] = [];
        const refinementRows: (string | number)[] = [];
        for (const sq of squadsData) {
            const sid = sq.squad.SquadID;
            for (const m of sq.members) {
                memberRows.push(sid, m.MemberID, m.ServerID, m.AnonymousUsers);
            }
            for (const r of sq.relics) {
                relicRows.push(sid, r.RelicID, r.Offcycle);
            }
            for (const ref of sq.refinements) {
                refinementRows.push(sid, ref.RefinementID, ref.Offcycle);
            }
        }
        if (memberRows.length > 0) {
            const ph = memberRows.length / 4;
            const placeholders = Array(ph).fill('(?, ?, ?, ?)').join(', ');
            await connection.execute(
                `INSERT INTO ${TABLES.SQUADUSERS} (SquadID, MemberID, ServerID, AnonymousUsers) VALUES ${placeholders}`,
                memberRows
            );
        }
        if (relicRows.length > 0) {
            const ph = relicRows.length / 3;
            const placeholders = Array(ph).fill('(?, ?, ?)').join(', ');
            await connection.execute(
                `INSERT INTO ${TABLES.SQUADRELICS} (SquadID, RelicID, Offcycle) VALUES ${placeholders}`,
                relicRows
            );
        }
        if (refinementRows.length > 0) {
            const ph = refinementRows.length / 3;
            const placeholders = Array(ph).fill('(?, ?, ?)').join(', ');
            await connection.execute(
                `INSERT INTO ${TABLES.SQUADREFINEMENT} (SquadID, RefinementID, Offcycle) VALUES ${placeholders}`,
                refinementRows
            );
        }
        await connection.commit();
    } catch (error) {
        await connection.rollback();
        throw error;
    } finally {
        connection.release();
    }
}

/** Get a single squaduser row, or undefined if not found. */
export async function getSquadUser(squadId: string, memberId: number): Promise<ISquadUserRow | undefined> {
    const rows = await SelectQuery<ISquadUserRow>(
        `SELECT * FROM ${TABLES.SQUADUSERS} WHERE SquadID = ? AND MemberID = ?`,
        [squadId, memberId]
    );
    return rows[0];
}

/** Get all squad IDs that a member is in (any squad, any age). */
export async function getSquadIdsByMemberId(memberId: number): Promise<string[]> {
    const rows = await SelectQuery<ISquadUserRow>(
        `SELECT SquadID FROM ${TABLES.SQUADUSERS} WHERE MemberID = ?`,
        [memberId]
    );
    return [...new Set(rows.map((r) => r.SquadID))];
}

/** Get squad IDs that a member is in, limited to active squads created in the last 24 hours. */
export async function getActiveSquadIdsByMemberId(memberId: number): Promise<string[]> {
    const cutoff = Date.now() - ACTIVE_SQUADS_CREATED_SINCE_MS;
    const rows = await SelectQuery<{ SquadID: string }>(
        `SELECT su.SquadID FROM ${TABLES.SQUADUSERS} su
         INNER JOIN ${TABLES.SQUADS} s ON su.SquadID = s.SquadID
         WHERE su.MemberID = ? AND s.Active = 1 AND s.CreatedAt >= ?`,
        [memberId, cutoff]
    );
    return rows.map((r) => r.SquadID);
}

/** Batch fetch everything needed for the leave loop: member's rows in these squads, and each squad's host + users. 3 queries total. */
export async function getLeaveContextBatch(
    memberId: number,
    squadIds: string[]
): Promise<{
    userRowsBySquadId: Map<string, ISquadUserRow>;
    squadHostAndUsers: Map<string, { host: number; users: ISquadUserRow[] }>;
}> {
    if (squadIds.length === 0) {
        return { userRowsBySquadId: new Map(), squadHostAndUsers: new Map() };
    }
    const placeholders = squadIds.map(() => '?').join(',');
    const [memberRows, squadRows, allSquadUsers] = await Promise.all([
        SelectQuery<ISquadUserRow>(
            `SELECT * FROM ${TABLES.SQUADUSERS} WHERE MemberID = ? AND SquadID IN (${placeholders})`,
            [memberId, ...squadIds]
        ),
        SelectQuery<{ SquadID: string; Host: number }>(
            `SELECT SquadID, Host FROM ${TABLES.SQUADS} WHERE SquadID IN (${placeholders})`,
            squadIds
        ),
        SelectQuery<ISquadUserRow>(
            `SELECT * FROM ${TABLES.SQUADUSERS} WHERE SquadID IN (${placeholders})`,
            squadIds
        )
    ]);
    const userRowsBySquadId = new Map<string, ISquadUserRow>();
    for (const r of memberRows) {
        userRowsBySquadId.set(r.SquadID, r);
    }
    const hostBySquadId = new Map(squadRows.map((r) => [r.SquadID, r.Host]));
    const usersBySquadId = new Map<string, ISquadUserRow[]>();
    for (const r of allSquadUsers) {
        const list = usersBySquadId.get(r.SquadID) ?? [];
        list.push(r);
        usersBySquadId.set(r.SquadID, list);
    }
    const squadHostAndUsers = new Map<string, { host: number; users: ISquadUserRow[] }>();
    for (const sid of squadIds) {
        const host = hostBySquadId.get(sid);
        const users = usersBySquadId.get(sid) ?? [];
        if (host !== undefined) {
            squadHostAndUsers.set(sid, { host, users });
        }
    }
    return { userRowsBySquadId, squadHostAndUsers };
}

/** Add a member to a squad (insert squaduser, increment CurrentCount). Fails if already in squad. */
export async function addSquadMember(squadId: string, memberId: number, serverId: number, anonymousUsers = 0): Promise<void> {
    await ModifyQuery(
        `INSERT INTO ${TABLES.SQUADUSERS} (SquadID, MemberID, ServerID, AnonymousUsers) VALUES (?, ?, ?, ?)`,
        [squadId, memberId, serverId, anonymousUsers]
    );
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADS} SET CurrentCount = CurrentCount + ? WHERE SquadID = ?`,
        [1 + anonymousUsers, squadId]
    );
}

/** Add one anonymous guest to a squad, attributed to addedByMemberId (e.g. host). Row must exist. */
export async function addSquadGuest(squadId: string, addedByMemberId: number): Promise<void> {
    const [res] = await pool.execute(
        `UPDATE ${TABLES.SQUADUSERS} SET AnonymousUsers = AnonymousUsers + 1 WHERE SquadID = ? AND MemberID = ?`,
        [squadId, addedByMemberId]
    );
    const header = res as ResultSetHeader;
    if (header.affectedRows === 0) {
        throw new Error('Squad user row not found');
    }
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADS} SET CurrentCount = CurrentCount + 1 WHERE SquadID = ?`,
        [squadId]
    );
}

/** Remove a member from a squad. When knownAnonymousUsers is provided, skips fetching the row. */
export async function removeSquadMember(
    squadId: string,
    memberId: number,
    knownAnonymousUsers?: number
): Promise<{ anonymousUsers: number }> {
    let anonymousUsers: number;
    if (knownAnonymousUsers !== undefined) {
        anonymousUsers = knownAnonymousUsers;
    } else {
        const row = await getSquadUser(squadId, memberId);
        if (!row) throw new Error('Member not in squad');
        anonymousUsers = row.AnonymousUsers ?? 0;
    }
    await ModifyQuery(
        `DELETE FROM ${TABLES.SQUADUSERS} WHERE SquadID = ? AND MemberID = ?`,
        [squadId, memberId]
    );
    const delta = 1 + anonymousUsers;
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADS} SET CurrentCount = GREATEST(0, CurrentCount - ?) WHERE SquadID = ?`,
        [delta, squadId]
    );
    return { anonymousUsers };
}

/** Remove one guest from a member's row. When knownAnonymousUsers is provided (and >= 1), skips fetching the row. */
export async function removeSquadGuest(
    squadId: string,
    memberId: number,
    knownAnonymousUsers?: number
): Promise<number> {
    let current: number;
    if (knownAnonymousUsers !== undefined) {
        current = knownAnonymousUsers;
        if (current < 1) throw new Error('No guests to remove');
    } else {
        const row = await getSquadUser(squadId, memberId);
        if (!row) throw new Error('Squad user row not found');
        current = row.AnonymousUsers ?? 0;
        if (current < 1) throw new Error('No guests to remove');
    }
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADUSERS} SET AnonymousUsers = AnonymousUsers - 1 WHERE SquadID = ? AND MemberID = ?`,
        [squadId, memberId]
    );
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADS} SET CurrentCount = CurrentCount - 1 WHERE SquadID = ?`,
        [squadId]
    );
    return current - 1;
}

/** Set new host and mark rehost. */
export async function updateSquadHost(squadId: string, newHostMemberId: number): Promise<void> {
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADS} SET Host = ?, Rehost = 1 WHERE SquadID = ?`,
        [newHostMemberId, squadId]
    );
}

/** Mark squad as closed and inactive. */
export async function setSquadClosedAndInactive(squadId: string, closedAt: number): Promise<void> {
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADS} SET Active = 0, ClosedAt = ?, CurrentCount = 0 WHERE SquadID = ?`,
        [closedAt, squadId]
    );
}

export interface LeaveBulkOps {
    guestRemovals: { squadId: string; memberId: number }[];
    nonHostLeaves: { squadId: string; memberId: number; anonymousUsers: number }[];
    hostRehosts: { squadId: string; memberId: number; anonymousUsers: number; newHostMemberId: number }[];
    hostCloses: { squadId: string; memberId: number; anonymousUsers: number; closedAt: number }[];
}

/** Execute all leave operations in one transaction with batched queries. */
export async function executeLeaveBulk(ops: LeaveBulkOps): Promise<void> {
    const { guestRemovals, nonHostLeaves, hostRehosts, hostCloses } = ops;
    const allDeletes = [
        ...nonHostLeaves.map((o) => [o.squadId, o.memberId] as const),
        ...hostRehosts.map((o) => [o.squadId, o.memberId] as const),
        ...hostCloses.map((o) => [o.squadId, o.memberId] as const)
    ];
    if (allDeletes.length === 0 && guestRemovals.length === 0) return;

    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();
        if (allDeletes.length > 0) {
            const placeholders = allDeletes.map(() => '(?,?)').join(',');
            const params = allDeletes.flat();
            await connection.execute(
                `DELETE FROM ${TABLES.SQUADUSERS} WHERE (SquadID, MemberID) IN (${placeholders})`,
                params
            );
        }
        if (guestRemovals.length > 0) {
            const placeholders = guestRemovals.map(() => '(?,?)').join(',');
            const params = guestRemovals.flatMap((o) => [o.squadId, o.memberId]);
            await connection.execute(
                `UPDATE ${TABLES.SQUADUSERS} SET AnonymousUsers = AnonymousUsers - 1 WHERE (SquadID, MemberID) IN (${placeholders})`,
                params
            );
            const squadIds = guestRemovals.map((o) => o.squadId);
            const ph = squadIds.map(() => '?').join(',');
            await connection.execute(
                `UPDATE ${TABLES.SQUADS} SET CurrentCount = CurrentCount - 1 WHERE SquadID IN (${ph})`,
                squadIds
            );
        }
        if (nonHostLeaves.length > 0) {
            const squadIds = nonHostLeaves.map((o) => o.squadId);
            const deltas = nonHostLeaves.map((o) => 1 + o.anonymousUsers);
            const cases = squadIds.map((id, i) => `WHEN ? THEN ?`).join(' ');
            const params = squadIds.flatMap((id, i) => [id, deltas[i]]).concat(squadIds);
            await connection.execute(
                `UPDATE ${TABLES.SQUADS} SET CurrentCount = GREATEST(0, CurrentCount - CASE SquadID ${cases} END) WHERE SquadID IN (${squadIds.map(() => '?').join(',')})`,
                params
            );
        }
        if (hostRehosts.length > 0) {
            const squadIds = hostRehosts.map((o) => o.squadId);
            const deltas = hostRehosts.map((o) => 1 + o.anonymousUsers);
            const newHosts = hostRehosts.map((o) => o.newHostMemberId);
            const caseCount = squadIds.map((_, i) => `WHEN ? THEN ?`).join(' ');
            const caseHost = squadIds.map((_, i) => `WHEN ? THEN ?`).join(' ');
            const params = [
                ...squadIds.flatMap((id, i) => [id, deltas[i]]),
                ...squadIds.flatMap((id, i) => [id, newHosts[i]]),
                ...squadIds
            ];
            await connection.execute(
                `UPDATE ${TABLES.SQUADS} SET CurrentCount = GREATEST(0, CurrentCount - CASE SquadID ${caseCount} END), Host = CASE SquadID ${caseHost} END, Rehost = 1 WHERE SquadID IN (${squadIds.map(() => '?').join(',')})`,
                params
            );
        }
        if (hostCloses.length > 0) {
            const squadIds = hostCloses.map((o) => o.squadId);
            const closedAts = hostCloses.map((o) => o.closedAt);
            const caseClosed = squadIds.map((_, i) => `WHEN ? THEN ?`).join(' ');
            const params = squadIds.flatMap((id, i) => [id, closedAts[i]]).concat(squadIds);
            await connection.execute(
                `UPDATE ${TABLES.SQUADS} SET Active = 0, ClosedAt = CASE SquadID ${caseClosed} END, CurrentCount = 0 WHERE SquadID IN (${squadIds.map(() => '?').join(',')})`,
                params
            );
        }
        await connection.commit();
    } catch (err) {
        await connection.rollback();
        throw err;
    } finally {
        connection.release();
    }
}
