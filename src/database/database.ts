import mysql, { ResultSetHeader } from 'mysql2/promise';
import crypto from 'crypto';
import dotenv from 'dotenv';
import IMemberRow from '../entities/db.member';
import ISquadRow, { ISquadPostRow, ISquadUserRow, ISquadRelicRow, ISquadRefinementRow } from '../entities/db.squads';
import IRelicRow from '../entities/db.relics';
import IPrimeSetRow from '../entities/db.primeSets';
import IPrimePartRow from '../entities/db.primeParts';
import IRefinementRow from '../entities/db.refinement';
import IRelicDropDataRow from '../entities/db.relicDropData';
import TABLES from '../entities/constants';
import { max } from 'underscore';

dotenv.config();

const dbDebugger = require('debug')('app:db');

const pool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE
});
dbDebugger('Connected to the database...');


export async function SelectQuery<T>(queryString: string, params?: (number | string | null)[]): Promise<T[]> {
    const [results] = await pool.execute(queryString, params);
    dbDebugger(`SelectQuery: ${queryString} using the params: ${params}`);
    return results as T[];
}

export async function ModifyQuery(queryString: string, params?: (number | string | null)[]): Promise<ResultSetHeader> {
    const [results] = await pool.execute(queryString, params);
    dbDebugger(`ModifyQuery: ${queryString} using the params: ${params}`);
    return results as ResultSetHeader;
}

// --- Auth: users and sessions ---

export interface IUserRow {
    id: number;
    username: string;
    password_hash: string;
    created_at: number;
    member_id?: number | null;
}

export interface ISessionRow {
    id: string;
    user_id: number;
    created_at: number;
    expires_at: number;
}

export async function getUserByUsername(username: string): Promise<IUserRow | null> {
    const rows = await SelectQuery<IUserRow>(
        `SELECT id, username, password_hash, created_at, member_id FROM ${TABLES.USERS} WHERE username = ?`,
        [username]
    );
    return rows[0] ?? null;
}

export async function createUser(username: string, passwordHash: string): Promise<number> {
    const now = Math.floor(Date.now() / 1000);
    await ModifyQuery(
        `INSERT INTO ${TABLES.USERS} (username, password_hash, created_at) VALUES (?, ?, ?)`,
        [username, passwordHash, now]
    );
    const [rows] = await pool.execute<mysql.RowDataPacket[]>(
        `SELECT LAST_INSERT_ID() AS id`
    );
    return Number(rows[0]?.id ?? 0);
}

export async function createSession(userId: number, expiresAt: number): Promise<string> {
    const id = crypto.randomBytes(32).toString('hex');
    await ModifyQuery(
        `INSERT INTO ${TABLES.SESSIONS} (id, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)`,
        [id, userId, Math.floor(Date.now() / 1000), expiresAt]
    );
    return id;
}

export async function getSessionById(sessionId: string): Promise<ISessionRow | null> {
    const now = Math.floor(Date.now() / 1000);
    const rows = await SelectQuery<ISessionRow>(
        `SELECT id, user_id, created_at, expires_at FROM ${TABLES.SESSIONS} WHERE id = ? AND expires_at > ?`,
        [sessionId, now]
    );
    return rows[0] ?? null;
}

export async function deleteSessionById(sessionId: string): Promise<void> {
    await ModifyQuery(`DELETE FROM ${TABLES.SESSIONS} WHERE id = ?`, [sessionId]);
}

export async function getUserById(id: number): Promise<IUserRow | null> {
    const rows = await SelectQuery<IUserRow>(
        `SELECT id, username, password_hash, created_at, member_id FROM ${TABLES.USERS} WHERE id = ?`,
        [id]
    );
    return rows[0] ?? null;
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

export async function getMemberByDiscordId(discordId: string | number) {
    return await SelectQuery<IMemberRow>(`SELECT * FROM ${TABLES.MEMBERS} WHERE DiscordID = ?`, [String(discordId)]);
}

/** Update the member linked to a user. Pass null to unlink. */
export async function updateUserMemberLink(userId: number, memberId: number | null): Promise<void> {
    await ModifyQuery(`UPDATE ${TABLES.USERS} SET member_id = ? WHERE id = ?`, [memberId, userId]);
}

/** Get the user (if any) linked to this member. Used to enforce one account per member. */
export async function getUserByMemberId(memberId: number): Promise<IUserRow | null> {
    const rows = await SelectQuery<IUserRow>(
        `SELECT id, username, password_hash, created_at, member_id FROM ${TABLES.USERS} WHERE member_id = ?`,
        [memberId]
    );
    return rows[0] ?? null;
}

/** Resolve a member by id, discord id (string to avoid precision loss), or name (first provided wins). Returns single member or null. */
export async function resolveMember(
  by: { memberId?: number; discordId?: string | number; memberName?: string }
): Promise<IMemberRow | null> {
  if (by.memberId != null) {
    const rows = await getMemberById(by.memberId);
    return rows[0] ?? null;
  }
  if (by.discordId != null && (typeof by.discordId !== 'string' || by.discordId.trim() !== '')) {
    const rows = await getMemberByDiscordId(by.discordId);
    return rows[0] ?? null;
  }
  if (by.memberName != null && String(by.memberName).trim() !== '') {
    const rows = await getMemberByName(by.memberName);
    return rows[0] ?? null;
  }
  return null;
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

/** Cutoff: only squads created in the last 24 hours. CreatedAt is stored in Unix seconds.
 *  For best performance, add an index on squads: (Active, CreatedAt). */
const ACTIVE_SQUADS_CREATED_SINCE_SEC = 24 * 60 * 60;

export async function getActiveSquads() {
    const cutoffSec = Math.floor(Date.now() / 1000) - ACTIVE_SQUADS_CREATED_SINCE_SEC;
    const squads = await SelectQuery<ISquadRow>(
        `SELECT * FROM ${TABLES.SQUADS} WHERE Active = 1 AND CreatedAt >= ? ORDER BY CreatedAt DESC`,
        [cutoffSec]
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

/** Paginated squads: same shape as getActiveSquads but with LIMIT/OFFSET and optional simple filters. Use for GET /api/squads when no member/relic/refinement filters. */
export interface GetSquadsPaginatedOpts {
    status: 'all' | 'active';
    limit: number;
    offset: number;
    era?: string;
    style?: string;
    hostMemberId?: number;
    originatingServerId?: number;
    filled?: number;
    /** When true, only squads that have at least one offcycle relic. */
    hasOffcycleRelics?: boolean;
}

export async function getSquadsPaginated(opts: GetSquadsPaginatedOpts) {
    const { status, limit, offset, era, style, hostMemberId, originatingServerId, filled, hasOffcycleRelics } = opts;
    const conditions: string[] = [];
    const params: (number | string)[] = [];

    if (hasOffcycleRelics) {
        conditions.push(`EXISTS (SELECT 1 FROM ${TABLES.SQUADRELICS} sr WHERE sr.SquadID = ${TABLES.SQUADS}.SquadID AND sr.Offcycle = 1)`);
    }
    if (status === 'active') {
        const cutoffSec = Math.floor(Date.now() / 1000) - ACTIVE_SQUADS_CREATED_SINCE_SEC;
        conditions.push('Active = 1', 'CreatedAt >= ?');
        params.push(cutoffSec);
    }
    if (era != null && era !== '') {
        conditions.push('Era = ?');
        params.push(era);
    }
    if (style != null && style !== '') {
        conditions.push('Style = ?');
        params.push(style);
    }
    if (hostMemberId != null) {
        conditions.push('Host = ?');
        params.push(hostMemberId);
    }
    if (originatingServerId != null) {
        conditions.push('OriginatingServer = ?');
        params.push(originatingServerId);
    }
    if (filled !== undefined && filled !== null) {
        conditions.push('Filled = ?');
        params.push(filled);
    }

    const whereClause = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const safeLimit = Math.min(Math.max(0, Math.floor(Number(limit)) || 0), 10000);
    const safeOffset = Math.max(0, Math.floor(Number(offset)) || 0);
    const sql = `SELECT * FROM ${TABLES.SQUADS} ${whereClause} ORDER BY CreatedAt DESC LIMIT ${safeLimit} OFFSET ${safeOffset}`;
    const squads = await SelectQuery<ISquadRow>(sql, params);

    if (squads.length === 0) {
        return { squads: [], squadUsers: [], squadRelics: [], squadRefinements: [], squadPosts: [] };
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

/** Options for getSquadsByIds. skipPosts: true avoids the squadposts query (e.g. for backfill). */
export interface GetSquadsByIdsOptions {
    skipPosts?: boolean;
}

/** Fetch multiple squads and their related rows. By default 5 queries; with skipPosts: true, 4 queries. Returns same shape as getSquadById but with arrays for multiple IDs. */
export async function getSquadsByIds(
    squadIds: string[],
    opts?: GetSquadsByIdsOptions
): Promise<{ squads: ISquadRow[]; squadUsers: ISquadUserRow[]; squadRelics: ISquadRelicRow[]; squadRefinements: ISquadRefinementRow[]; squadPosts: ISquadPostRow[] }> {
    if (squadIds.length === 0) {
        return { squads: [], squadUsers: [], squadRelics: [], squadRefinements: [], squadPosts: [] };
    }
    const placeholders = squadIds.map(() => '?').join(',');
    const skipPosts = opts?.skipPosts === true;
    const [squads, squadUsers, squadRelics, squadRefinements, squadPosts] = await Promise.all([
        SelectQuery<ISquadRow>(`SELECT * FROM ${TABLES.SQUADS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadUserRow>(`SELECT * FROM ${TABLES.SQUADUSERS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadRelicRow>(`SELECT * FROM ${TABLES.SQUADRELICS} WHERE SquadID IN (${placeholders})`, squadIds),
        SelectQuery<ISquadRefinementRow>(`SELECT * FROM ${TABLES.SQUADREFINEMENT} WHERE SquadID IN (${placeholders})`, squadIds),
        skipPosts ? Promise.resolve([] as ISquadPostRow[]) : SelectQuery<ISquadPostRow>(`SELECT * FROM ${TABLES.SQUADPOSTS} WHERE SquadID IN (${placeholders})`, squadIds)
    ]);
    return { squads, squadUsers, squadRelics, squadRefinements, squadPosts };
}

/** Count closed squads per day (by ClosedAt date). Returns total, filled, and unfilled per day. Day is derived from ClosedAt (server timezone). Includes every day in the range (zeros for days with no squads). */
export interface SquadCountPerDay {
    date: string;
    total: number;
    filled: number;
    unfilled: number;
}

export type SquadCountPerDaySort = 'date' | 'filled' | 'total' | 'unfilled';

/** Parse sort param for counts-per-day: date (default), filled, total, unfilled. */
export function getSquadCountsPerDayOrderBy(sort: string): SquadCountPerDaySort {
    const s = (sort ?? '').toLowerCase();
    if (s === 'filled' || s === 'total' || s === 'unfilled') return s;
    return 'date';
}

/** Reputation (squad counts) per calendar day for one member. Same shape as SquadCountPerDay. */
export interface MemberReputationPerDay {
    date: string;
    total: number;
    filled: number;
    unfilled: number;
}

export type MemberReputationPerDaySort = 'date' | 'filled' | 'total' | 'unfilled';

/** Parse sort param for member reputation-per-day: date (default, ascending), filled, total, unfilled (others descending). */
export function getMemberReputationPerDayOrderBy(sort: string): MemberReputationPerDaySort {
    const s = (sort ?? '').toLowerCase();
    if (s === 'filled' || s === 'total' || s === 'unfilled') return s;
    return 'date';
}

/** Return reputation per day for a member (closed squads they participated in). Sorted by date ascending when sort=date, else by sort field descending. */
export async function getMemberReputationPerDay(
    memberId: number,
    sortBy: MemberReputationPerDaySort = 'date'
): Promise<MemberReputationPerDay[]> {
    const closedSecExpr = `IF(s.ClosedAt >= 10000000000, FLOOR(s.ClosedAt/1000), s.ClosedAt)`;
    const rows = await SelectQuery<mysql.RowDataPacket>(
        `SELECT DATE(FROM_UNIXTIME(${closedSecExpr})) AS day_date,
                COUNT(*) AS total,
                COALESCE(SUM(s.Filled), 0) AS filled
         FROM ${TABLES.SQUADS} s
         INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID AND su.MemberID = ?
         WHERE s.ClosedAt IS NOT NULL
         GROUP BY day_date`,
        [memberId]
    );
    const results: MemberReputationPerDay[] = (rows as { day_date: unknown; total: number; filled: number }[]).map((r) => {
        const total = Number(r.total);
        const filled = Number(r.filled);
        return { date: toDateString(r.day_date), total, filled, unfilled: total - filled };
    });
    if (sortBy === 'date') {
        results.sort((a, b) => a.date.localeCompare(b.date));
    } else {
        results.sort((a, b) => {
            const diff = (b[sortBy] as number) - (a[sortBy] as number);
            return diff !== 0 ? diff : a.date.localeCompare(b.date);
        });
    }
    return results;
}

function allDatesBetween(minDate: string, maxDate: string): string[] {
    const min = new Date(minDate);
    const max = new Date(maxDate);
    const out: string[] = [];
    const d = new Date(min);
    while (d <= max) {
        out.push(d.toISOString().slice(0, 10));
        d.setUTCDate(d.getUTCDate() + 1);
    }
    return out;
}

/** Normalize DB date (Date or string) to YYYY-MM-DD for consistent map keys. */
function toDateString(v: unknown): string {
    if (v instanceof Date) return v.toISOString().slice(0, 10);
    const s = String(v ?? '');
    return s.slice(0, 10);
}

export async function getSquadCountsPerDay(sortBy: SquadCountPerDaySort = 'date'): Promise<SquadCountPerDay[]> {
    const closedSecExpr = `IF(s.ClosedAt >= 10000000000, FLOOR(s.ClosedAt/1000), s.ClosedAt)`;
    const rows = await SelectQuery<mysql.RowDataPacket>(
        `SELECT DATE(FROM_UNIXTIME(${closedSecExpr})) AS day_date,
                COUNT(*) AS total,
                COALESCE(SUM(s.Filled), 0) AS filled
         FROM ${TABLES.SQUADS} s
         WHERE s.ClosedAt IS NOT NULL
         GROUP BY day_date`,
        []
    );
    const byDate = new Map<string, { total: number; filled: number; unfilled: number }>();
    let minDate: string | null = null;
    let maxDate: string | null = null;
    for (const r of rows as { day_date: unknown; total: number; filled: number }[]) {
        const total = Number(r.total);
        const filled = Number(r.filled);
        const date = toDateString(r.day_date);
        byDate.set(date, { total, filled, unfilled: total - filled });
        if (minDate == null || date < minDate) minDate = date;
        if (maxDate == null || date > maxDate) maxDate = date;
    }
    let dates: string[];
    if (minDate != null && maxDate != null) {
        dates = allDatesBetween(minDate, maxDate);
    } else {
        dates = [];
    }
    const results: SquadCountPerDay[] = dates.map((date) => {
        const row = byDate.get(date) ?? { total: 0, filled: 0, unfilled: 0 };
        return { date, total: row.total, filled: row.filled, unfilled: row.unfilled };
    });
    if (sortBy === 'date') {
        results.sort((a, b) => a.date.localeCompare(b.date));
    } else {
        results.sort((a, b) => {
            const diff = (b[sortBy] as number) - (a[sortBy] as number);
            return diff !== 0 ? diff : a.date.localeCompare(b.date);
        });
    }
    return results;
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

export interface PrimeSetWithParts extends IPrimeSetRow {
    parts: Array<{ Part: string; Price: number; Ducats: number; Required: number }>;
}

export async function getPrimeSetsWithParts(): Promise<PrimeSetWithParts[]> {
    const sets = await SelectQuery<IPrimeSetRow>(`SELECT * FROM ${TABLES.PRIMESETS}`);
    const parts = await SelectQuery<IPrimePartRow>(`SELECT * FROM ${TABLES.PRIMEPARTS}`);
    const partsBySet = new Map<string, IPrimePartRow[]>();
    for (const p of parts) {
        const list = partsBySet.get(p.PrimeSet) ?? [];
        list.push(p);
        partsBySet.set(p.PrimeSet, list);
    }
    return sets.map((s) => ({
        ...s,
        parts: (partsBySet.get(s.PrimeSet) ?? []).map((p) => ({
            Part: p.Part,
            Price: p.Price,
            Ducats: p.Ducats,
            Required: p.Required
        }))
    }));
}

export async function getPrimeSetByName(setName: string): Promise<PrimeSetWithParts | null> {
    const rows = await SelectQuery<IPrimeSetRow>(
        `SELECT * FROM ${TABLES.PRIMESETS} WHERE PrimeSet = ?`,
        [setName]
    );
    const set = rows[0];
    if (!set) return null;
    const partRows = await SelectQuery<IPrimePartRow>(
        `SELECT * FROM ${TABLES.PRIMEPARTS} WHERE PrimeSet = ?`,
        [setName]
    );
    return {
        ...set,
        parts: partRows.map((p) => ({
            Part: p.Part,
            Price: p.Price,
            Ducats: p.Ducats,
            Required: p.Required
        }))
    };
}

export async function getPrimePart(setName: string, partName: string): Promise<IPrimePartRow | null> {
    const rows = await SelectQuery<IPrimePartRow>(
        `SELECT * FROM ${TABLES.PRIMEPARTS} WHERE PrimeSet = ? AND Part = ?`,
        [setName, partName]
    );
    return rows[0] ?? null;
}

export interface RelicDropRow {
    partName: string;
    rarity: string;
    ducats: number;
    price: number;
    chances?: Record<string, number> | null;
}

function mapDropRow(r: IRelicDropDataRow): RelicDropRow {
    return {
        partName: r.PartName,
        rarity: r.Rarity,
        ducats: r.Ducats,
        price: r.Price,
        chances: r.Chances != null ? (typeof r.Chances === 'string' ? JSON.parse(r.Chances) : r.Chances) : null
    };
}

export async function getRelicDrops(relicId: number): Promise<RelicDropRow[]> {
    const rows = await SelectQuery<IRelicDropDataRow>(
        `SELECT PartName, Rarity, Ducats, Price, Chances FROM ${TABLES.RELICDROPDATA} WHERE RelicID = ?`,
        [relicId]
    );
    return rows.map(mapDropRow);
}

export async function getRelicDropsByRelicIds(relicIds: number[]): Promise<Map<number, RelicDropRow[]>> {
    const map = new Map<number, RelicDropRow[]>();
    if (relicIds.length === 0) return map;
    const placeholders = relicIds.map(() => '?').join(',');
    const rows = await SelectQuery<IRelicDropDataRow & { RelicID: number }>(
        `SELECT RelicID, PartName, Rarity, Ducats, Price, Chances FROM ${TABLES.RELICDROPDATA} WHERE RelicID IN (${placeholders})`,
        relicIds
    );
    for (const r of rows) {
        const list = map.get(r.RelicID) ?? [];
        list.push(mapDropRow(r));
        map.set(r.RelicID, list);
    }
    return map;
}

export interface RelrunSyncPayload {
    primeSets: Array<{
        PrimeSet: string;
        Price: number;
        Ducats: number;
        PartsTotalPrice: number;
        Category: string;
        Vaulted: number;
    }>;
    primeParts: Array<{
        PrimeSet: string;
        Part: string;
        Price: number;
        Ducats: number;
        Required: number;
    }>;
    relicDrops: Array<{
        RelicID: number;
        PartName: string;
        Rarity: string;
        Ducats: number;
        Price: number;
        Chances: string | null;
    }>;
    relicVaultedById: Array<{ RelicID: number; Vaulted: number }>;
}

export async function runRelrunSync(payload: RelrunSyncPayload): Promise<void> {
    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();
        const { primeSets, primeParts, relicDrops, relicVaultedById } = payload;

        if (primeSets.length > 0) {
            await conn.execute(
                `INSERT INTO ${TABLES.PRIMESETS} (PrimeSet, Price, Ducats, PartsTotalPrice, PrimeAccess, Category, Vaulted)
                 VALUES ${primeSets.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ')}
                 ON DUPLICATE KEY UPDATE Price = VALUES(Price), Ducats = VALUES(Ducats), PartsTotalPrice = VALUES(PartsTotalPrice), Category = VALUES(Category), Vaulted = VALUES(Vaulted)`,
                primeSets.flatMap((r) => [r.PrimeSet, r.Price, r.Ducats, r.PartsTotalPrice, '', r.Category, r.Vaulted])
            );
        }

        if (primeParts.length > 0) {
            await conn.execute(
                `INSERT INTO ${TABLES.PRIMEPARTS} (PrimeSet, Part, Price, Ducats, Required)
                 VALUES ${primeParts.map(() => '(?, ?, ?, ?, ?)').join(', ')}
                 ON DUPLICATE KEY UPDATE Price = VALUES(Price), Ducats = VALUES(Ducats), Required = VALUES(Required)`,
                primeParts.flatMap((r) => [r.PrimeSet, r.Part, r.Price, r.Ducats, r.Required])
            );
        }

        for (const { RelicID, Vaulted } of relicVaultedById) {
            await conn.execute(
                `UPDATE ${TABLES.RELICS} SET Vaulted = ? WHERE ID = ?`,
                [Vaulted, RelicID]
            );
        }

        await conn.execute(`DELETE FROM ${TABLES.RELICDROPDATA}`);
        if (relicDrops.length > 0) {
            const batchSize = 500;
            for (let i = 0; i < relicDrops.length; i += batchSize) {
                const batch = relicDrops.slice(i, i + batchSize);
                await conn.execute(
                    `INSERT INTO ${TABLES.RELICDROPDATA} (RelicID, PartName, Rarity, Ducats, Price, Chances)
                     VALUES ${batch.map(() => '(?, ?, ?, ?, ?, ?)').join(', ')}`,
                    batch.flatMap((r) => [r.RelicID, r.PartName, r.Rarity, r.Ducats, r.Price, r.Chances])
                );
            }
        }

        await conn.commit();
    } catch (e) {
        await conn.rollback();
        throw e;
    } finally {
        conn.release();
    }
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
    const cutoffSec = Math.floor(Date.now() / 1000) - ACTIVE_SQUADS_CREATED_SINCE_SEC;
    const rows = await SelectQuery<{ SquadID: string }>(
        `SELECT su.SquadID FROM ${TABLES.SQUADUSERS} su
         INNER JOIN ${TABLES.SQUADS} s ON su.SquadID = s.SquadID
         WHERE su.MemberID = ? AND s.Active = 1 AND s.CreatedAt >= ?`,
        [memberId, cutoffSec]
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

/** Mark squad as closed and inactive. Sets Filled and CloseReason when provided. closedAt should be Unix seconds (same as squads.ClosedAt). */
export async function setSquadClosedAndInactive(
    squadId: string,
    closedAt: number,
    options?: { filled?: number; closeReason?: string | null }
): Promise<void> {
    const filled = options?.filled ?? 0;
    const closeReason = options?.closeReason ?? null;
    await ModifyQuery(
        `UPDATE ${TABLES.SQUADS} SET Active = 0, ClosedAt = ?, CurrentCount = 0, Filled = ?, CloseReason = ? WHERE SquadID = ?`,
        [closedAt, filled, closeReason, squadId]
    );
}

export interface LeaveBulkOps {
    guestRemovals: { squadId: string; memberId: number }[];
    nonHostLeaves: { squadId: string; memberId: number; anonymousUsers: number }[];
    hostRehosts: { squadId: string; memberId: number; anonymousUsers: number; newHostMemberId: number }[];
    hostCloses: { squadId: string; memberId: number; anonymousUsers: number; closedAt: number; filled: number; closeReason: string }[];
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
            const filleds = hostCloses.map((o) => o.filled);
            const closeReasons = hostCloses.map((o) => o.closeReason);
            const caseClosed = squadIds.map(() => `WHEN ? THEN ?`).join(' ');
            const caseFilled = squadIds.map(() => `WHEN ? THEN ?`).join(' ');
            const caseReason = squadIds.map(() => `WHEN ? THEN ?`).join(' ');
            const params = [
                ...squadIds.flatMap((id, i) => [id, closedAts[i]]),
                ...squadIds.flatMap((id, i) => [id, filleds[i]]),
                ...squadIds.flatMap((id, i) => [id, closeReasons[i]]),
                ...squadIds
            ];
            await connection.execute(
                `UPDATE ${TABLES.SQUADS} SET Active = 0, ClosedAt = CASE SquadID ${caseClosed} END, CurrentCount = 0, Filled = CASE SquadID ${caseFilled} END, CloseReason = CASE SquadID ${caseReason} END WHERE SquadID IN (${squadIds.map(() => '?').join(',')})`,
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

// --- Reputation: 20 min cooldown for filled counts; TotalSquads always incremented ---
const MEMBER_REPUTATION_COOLDOWN_MS = 20 * 60 * 1000;
/** LastUpdate is stored as Unix seconds (INT); use this for DB writes. */
const MEMBER_REPUTATION_COOLDOWN_SEC = Math.floor(MEMBER_REPUTATION_COOLDOWN_MS / 1000);
const ROLLING_MS = {
    day: 24 * 60 * 60 * 1000,
    week: 7 * 24 * 60 * 60 * 1000,
    month: 30 * 24 * 60 * 60 * 1000,
    threeMonth: 90 * 24 * 60 * 60 * 1000,
    sixMonth: 180 * 24 * 60 * 60 * 1000,
    year: 365 * 24 * 60 * 60 * 1000
};

function buildHostSignatureFromRows(
    squad: ISquadRow,
    relics: ISquadRelicRow[],
    refinements: ISquadRefinementRow[]
): string {
    const relicPart = relics
        .map((r) => `${r.RelicID}:${r.Offcycle ?? 0}`)
        .sort()
        .join(',');
    const refPart = refinements
        .map((r) => `${r.RefinementID}:${r.Offcycle ?? 0}`)
        .sort()
        .join(',');
    const cycle =
        squad.CycleRequirement !== undefined && squad.CycleRequirement !== null
            ? String(squad.CycleRequirement)
            : '';
    return `${squad.Era ?? ''}|${squad.Style ?? ''}|${cycle}|${relicPart}|${refPart}`;
}

/** Parsed host Display string: Era|Style|Cycle|relicId:off,...|refId:off,... */
export interface ParsedHostDisplay {
    era: string;
    style: string;
    cycle: string;
    relicSegs: [number, number][];
    refSegs: [number, number][];
}

const HOST_DISPLAY_REGEX = /^([^|]*)\|([^|]*)\|([^|]*)\|([^|]*)\|(.*)$/;

export function parseHostDisplay(display: string): ParsedHostDisplay | null {
    if (!display) return null;
    const m = HOST_DISPLAY_REGEX.exec(display);
    if (!m) return null;
    const [, era = '', style = '', cycle = '', relicPart = '', refPart = ''] = m;
    const relicSegs: [number, number][] = [];
    for (const seg of relicPart.split(',').filter(Boolean)) {
        const [id, off] = seg.split(':').map(Number);
        if (!Number.isNaN(id)) relicSegs.push([id, off ?? 0]);
    }
    const refSegs: [number, number][] = [];
    for (const seg of refPart.split(',').filter(Boolean)) {
        const [id, off] = seg.split(':').map(Number);
        if (!Number.isNaN(id)) refSegs.push([id, off ?? 0]);
    }
    return { era, style, cycle, relicSegs, refSegs };
}

/** Format relic segments as "Era Name, Era Name, ..." (comma-separated, no collapsing). */
function formatRelicsList(
    segments: [number, number][],
    relicMap: Map<number, { era: string; name: string }>
): string {
    return segments
        .map(([id]) => {
            const r = relicMap.get(id);
            return r ? `${r.era} ${r.name}`.trim() : '';
        })
        .filter(Boolean)
        .join(', ');
}

/**
 * Build human-readable host display.
 * Format: [on-cycle relics] [style] [refinement(s)] [off-cycle relics (if any)] [off-cycle refinements (if any)] [cycles (if any)].
 * Relics are comma-separated (e.g. "Neo S2, Neo S10, Neo S13").
 */
export function formatHostDisplayReadable(
    parsed: ParsedHostDisplay | null,
    relicMap: Map<number, { era: string; name: string }>,
    refMap: Map<number, string>
): string {
    if (!parsed) return '';
    const onRelics = parsed.relicSegs.filter(([, off]) => off === 0);
    const offRelics = parsed.relicSegs.filter(([, off]) => off === 1);
    const onRefs = parsed.refSegs.filter(([, off]) => off === 0);
    const offRefs = parsed.refSegs.filter(([, off]) => off === 1);

    const parts: string[] = [];
    const onRelicStr = formatRelicsList(onRelics, relicMap);
    if (onRelicStr) parts.push(onRelicStr);
    if (parsed.style) parts.push(parsed.style);
    const onRefStr = onRefs.map(([id]) => refMap.get(id)).filter(Boolean).join(' ');
    if (onRefStr) parts.push(onRefStr);
    const offRelicStr = formatRelicsList(offRelics, relicMap);
    if (offRelicStr) parts.push(offRelicStr);
    const offRefStr = offRefs.map(([id]) => refMap.get(id)).filter(Boolean).join(' ');
    if (offRefStr) parts.push(offRefStr);
    if (offRelicStr) parts.push('offcycle');
    if (parsed.cycle) parts.push(`${parsed.cycle}+`);

    return parts.join(' ');
}

/** Resolve host Display string to human-readable form (fetches relic/refinement names). */
export async function expandHostDisplay(display: string): Promise<string> {
    const parsed = parseHostDisplay(display);
    if (!parsed) return display || '';
    const relicIds = [...new Set(parsed.relicSegs.map(([id]) => id))];
    const refIds = [...new Set(parsed.refSegs.map(([id]) => id))];
    const relicMap = new Map<number, { era: string; name: string }>();
    const refMap = new Map<number, string>();
    if (relicIds.length > 0) {
        const ph = relicIds.map(() => '?').join(',');
        const rows = await SelectQuery<IRelicRow>(`SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID IN (${ph})`, relicIds);
        rows.forEach((r) => relicMap.set(r.ID, { era: r.Era ?? '', name: r.Name ?? '' }));
    }
    if (refIds.length > 0) {
        const ph = refIds.map(() => '?').join(',');
        const rows = await SelectQuery<IRefinementRow>(`SELECT ID, Name FROM ${TABLES.REFINEMENT} WHERE ID IN (${ph})`, refIds);
        rows.forEach((r) => refMap.set(r.ID, r.Name ?? ''));
    }
    const readable = formatHostDisplayReadable(parsed, relicMap, refMap);
    return readable || display;
}

async function getOrCreateHost(
    connection: mysql.PoolConnection,
    signatureStr: string,
    closedAt: number
): Promise<number> {
    const hash = crypto.createHash('sha256').update(signatureStr, 'utf8').digest();
    const [existingRows] = await connection.execute<mysql.RowDataPacket[]>(
        `SELECT HostID FROM ${TABLES.HOSTS} WHERE SignatureHash = ?`,
        [hash]
    );
    const rows = Array.isArray(existingRows) ? existingRows : [];
    if (rows.length > 0) {
        return (rows[0] as { HostID: number }).HostID;
    }
    const style = (signatureStr.match(/^\w+\|([^|]*)/)?.[1] ?? null) || null;
    const [insertResult] = await connection.execute<ResultSetHeader>(
        `INSERT INTO ${TABLES.HOSTS} (SignatureHash, Display, Style, CreatedAt) VALUES (?, ?, ?, ?)`,
        [hash, signatureStr.slice(0, 1024), style, closedAt]
    );
    return (insertResult as ResultSetHeader).insertId ?? 0;
}

/** Normalize ClosedAt to Unix seconds. DB may store seconds or legacy milliseconds; LastUpdate is INT so must be seconds. */
function closedAtToSec(closedAt: number): number {
    return closedAt >= 1e10 ? Math.floor(closedAt / 1000) : Math.floor(closedAt);
}

/**
 * Normalize from/to query params. Fixes common typo: "?from=2026-02-01?to=2026-02-28" (second ? should be &).
 * Use: const { from, to } = normalizeFromTo(req.query); then parseDateRange(from, to).
 */
export function normalizeFromTo(query: Record<string, unknown>): { from: unknown; to: unknown } {
    let from = query.from;
    let to = query.to;
    if ((to == null || to === '') && typeof from === 'string' && from.includes('?to=')) {
        const [f, t] = from.split('?to=');
        from = f?.trim() ?? from;
        to = (t?.trim() ?? '') || to;
    }
    return { from, to };
}

/**
 * Parse optional date range for profile filtering. Squads are filtered by ClosedAt in [fromSec, toSec] (inclusive).
 * Returns null if either from or to is missing/invalid (no range filter).
 * Accepts: Unix timestamp (seconds or ms), or date string (YYYY-MM-DD or ISO).
 * - YYYY-MM-DD: "from" = start of that day UTC (00:00:00), "to" = end of that day UTC (23:59:59). Range is inclusive of both days.
 * - Timestamps / other strings: exact second is used.
 */
export function parseDateRange(from: unknown, to: unknown): { fromSec: number; toSec: number } | null {
    if (from == null || to == null || from === '' || to === '') return null;
    const toSec = (v: unknown): number | null => {
        if (typeof v === 'number' && Number.isFinite(v)) return v >= 1e12 ? Math.floor(v / 1000) : Math.floor(v);
        if (typeof v === 'string') {
            const d = new Date(v);
            if (Number.isNaN(d.getTime())) return null;
            return Math.floor(d.getTime() / 1000);
        }
        return null;
    };
    const fromSecVal = toSec(from);
    let toSecVal = toSec(to);
    if (fromSecVal == null || toSecVal == null) return null;
    if (typeof to === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(to.trim())) {
        toSecVal = toSecVal + 86400 - 1;
        if (toSecVal < fromSecVal) toSecVal = fromSecVal;
    }
    if (fromSecVal > toSecVal) return null;
    return { fromSec: fromSecVal, toSec: toSecVal };
}

/** closedAtSec: Unix timestamp in seconds (squads.ClosedAt). */
function getRollingIncrements(closedAtSec: number): { day: number; week: number; month: number; threeMonth: number; sixMonth: number; year: number } {
    const nowMs = Date.now();
    const closedAtMs = closedAtSec * 1000;
    return {
        day: closedAtMs >= nowMs - ROLLING_MS.day ? 1 : 0,
        week: closedAtMs >= nowMs - ROLLING_MS.week ? 1 : 0,
        month: closedAtMs >= nowMs - ROLLING_MS.month ? 1 : 0,
        threeMonth: closedAtMs >= nowMs - ROLLING_MS.threeMonth ? 1 : 0,
        sixMonth: closedAtMs >= nowMs - ROLLING_MS.sixMonth ? 1 : 0,
        year: closedAtMs >= nowMs - ROLLING_MS.year ? 1 : 0
    };
}

/** closedAtSec: Unix timestamp in seconds (squads.ClosedAt). */
async function upsertMemberReputation(
    connection: mysql.PoolConnection,
    memberId: number,
    closedAtSec: number,
    filled: number
): Promise<void> {
    const inc = getRollingIncrements(closedAtSec);
    const addFilled = filled === 1 ? 1 : 0;
    const cooldownSec = MEMBER_REPUTATION_COOLDOWN_SEC;

    const [existing] = await connection.execute<mysql.RowDataPacket[]>(
        `SELECT LastUpdate FROM ${TABLES.MEMBERREPUTATION} WHERE MemberID = ?`,
        [memberId]
    );
    const rows = Array.isArray(existing) ? existing : [];
    const lastUpdate = rows.length > 0 ? (rows[0].LastUpdate as number | null) : null;
    const applyFilled = addFilled === 1 && (lastUpdate == null || closedAtSec - lastUpdate >= cooldownSec);
    const dayInc = applyFilled ? inc.day : 0;
    const weekInc = applyFilled ? inc.week : 0;
    const monthInc = applyFilled ? inc.month : 0;
    const threeMonthInc = applyFilled ? inc.threeMonth : 0;
    const sixMonthInc = applyFilled ? inc.sixMonth : 0;
    const yearInc = applyFilled ? inc.year : 0;
    const allTimeInc = applyFilled ? 1 : 0;
    const newLastUpdate = applyFilled ? closedAtSec : (lastUpdate ?? closedAtSec);

    const filledSquadsInc = addFilled;

    if (rows.length === 0) {
        await connection.execute(
            `INSERT INTO ${TABLES.MEMBERREPUTATION} (MemberID, Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, FilledSquads, TotalSquads, LastUpdate)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`,
            [memberId, dayInc, weekInc, monthInc, threeMonthInc, sixMonthInc, yearInc, allTimeInc, filledSquadsInc, newLastUpdate]
        );
    } else {
        await connection.execute(
            `UPDATE ${TABLES.MEMBERREPUTATION} SET TotalSquads = TotalSquads + 1, FilledSquads = FilledSquads + ?,
             Day = Day + ?, Week = Week + ?, Month = Month + ?, ThreeMonth = ThreeMonth + ?, SixMonth = SixMonth + ?, Year = Year + ?,
             AllTime = AllTime + ?, LastUpdate = ? WHERE MemberID = ?`,
            [filledSquadsInc, dayInc, weekInc, monthInc, threeMonthInc, sixMonthInc, yearInc, allTimeInc, newLastUpdate, memberId]
        );
    }
}

async function upsertHostReputation(
    connection: mysql.PoolConnection,
    hostId: number,
    closedAtSec: number,
    filled: number
): Promise<void> {
    const inc = getRollingIncrements(closedAtSec);
    const addFilled = filled === 1 ? 1 : 0;

    const [existing] = await connection.execute<mysql.RowDataPacket[]>(
        `SELECT LastUpdate FROM ${TABLES.HOSTREPUTATION} WHERE HostID = ?`,
        [hostId]
    );
    const rows = Array.isArray(existing) ? existing : [];
    const lastUpdate = rows.length > 0 ? (rows[0].LastUpdate as number | null) : null;
    const applyFilled = addFilled === 1;
    const dayInc = applyFilled ? inc.day : 0;
    const weekInc = applyFilled ? inc.week : 0;
    const monthInc = applyFilled ? inc.month : 0;
    const threeMonthInc = applyFilled ? inc.threeMonth : 0;
    const sixMonthInc = applyFilled ? inc.sixMonth : 0;
    const yearInc = applyFilled ? inc.year : 0;
    const allTimeInc = applyFilled ? 1 : 0;
    const newLastUpdate = applyFilled ? closedAtSec : (lastUpdate ?? closedAtSec);

    if (rows.length === 0) {
        await connection.execute(
            `INSERT INTO ${TABLES.HOSTREPUTATION} (HostID, Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, TotalSquads, LastUpdate)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`,
            [hostId, dayInc, weekInc, monthInc, threeMonthInc, sixMonthInc, yearInc, allTimeInc, newLastUpdate]
        );
    } else {
        await connection.execute(
            `UPDATE ${TABLES.HOSTREPUTATION} SET TotalSquads = TotalSquads + 1,
             Day = Day + ?, Week = Week + ?, Month = Month + ?, ThreeMonth = ThreeMonth + ?, SixMonth = SixMonth + ?, Year = Year + ?,
             AllTime = AllTime + ?, LastUpdate = ? WHERE HostID = ?`,
            [dayInc, weekInc, monthInc, threeMonthInc, sixMonthInc, yearInc, allTimeInc, newLastUpdate, hostId]
        );
    }
}

async function upsertRelicReputation(
    connection: mysql.PoolConnection,
    relicId: number,
    closedAtSec: number,
    filled: number
): Promise<void> {
    const inc = getRollingIncrements(closedAtSec);
    const addFilled = filled === 1 ? 1 : 0;

    const [existing] = await connection.execute<mysql.RowDataPacket[]>(
        `SELECT LastUpdate FROM ${TABLES.RELICREPUTATION} WHERE RelicID = ?`,
        [relicId]
    );
    const rows = Array.isArray(existing) ? existing : [];
    const lastUpdate = rows.length > 0 ? (rows[0].LastUpdate as number | null) : null;
    const applyFilled = addFilled === 1;
    const dayInc = applyFilled ? inc.day : 0;
    const weekInc = applyFilled ? inc.week : 0;
    const monthInc = applyFilled ? inc.month : 0;
    const threeMonthInc = applyFilled ? inc.threeMonth : 0;
    const sixMonthInc = applyFilled ? inc.sixMonth : 0;
    const yearInc = applyFilled ? inc.year : 0;
    const allTimeInc = applyFilled ? 1 : 0;
    const newLastUpdate = applyFilled ? closedAtSec : (lastUpdate ?? closedAtSec);

    if (rows.length === 0) {
        await connection.execute(
            `INSERT INTO ${TABLES.RELICREPUTATION} (RelicID, Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, TotalSquads, LastUpdate)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)`,
            [relicId, dayInc, weekInc, monthInc, threeMonthInc, sixMonthInc, yearInc, allTimeInc, newLastUpdate]
        );
    } else {
        await connection.execute(
            `UPDATE ${TABLES.RELICREPUTATION} SET TotalSquads = TotalSquads + 1,
             Day = Day + ?, Week = Week + ?, Month = Month + ?, ThreeMonth = ThreeMonth + ?, SixMonth = SixMonth + ?, Year = Year + ?,
             AllTime = AllTime + ?, LastUpdate = ? WHERE RelicID = ?`,
            [dayInc, weekInc, monthInc, threeMonthInc, sixMonthInc, yearInc, allTimeInc, newLastUpdate, relicId]
        );
    }
}

/** closedAtSec: Unix timestamp in seconds (squads.ClosedAt). LastSquadAt stored in seconds. */
async function upsertMemberFriend(
    connection: mysql.PoolConnection,
    memberId1: number,
    memberId2: number,
    closedAtSec: number,
    filled: number
): Promise<void> {
    const [m1, m2] = memberId1 < memberId2 ? [memberId1, memberId2] : [memberId2, memberId1];
    const filledSquads = filled === 1 ? 1 : 0;
    await connection.execute(
        `INSERT INTO ${TABLES.MEMBERFRIENDS} (MemberID1, MemberID2, SquadsTogether, FilledSquads, LastSquadAt) VALUES (?, ?, 1, ?, ?)
         ON DUPLICATE KEY UPDATE SquadsTogether = SquadsTogether + 1, FilledSquads = FilledSquads + ?, LastSquadAt = ?`,
        [m1, m2, filledSquads, closedAtSec, filledSquads, closedAtSec]
    );
}

async function upsertRelicFriend(
    connection: mysql.PoolConnection,
    memberId: number,
    relicId: number,
    offcycle: number,
    closedAtSec: number,
    filled: number
): Promise<void> {
    const filledSquads = filled === 1 ? 1 : 0;
    const off = offcycle === 1 ? 1 : 0;
    await connection.execute(
        `INSERT INTO ${TABLES.RELICFRIENDS} (MemberID, RelicID, Offcycle, SquadsTogether, FilledSquads, LastSquadAt) VALUES (?, ?, ?, 1, ?, ?)
         ON DUPLICATE KEY UPDATE SquadsTogether = SquadsTogether + 1, FilledSquads = FilledSquads + ?, LastSquadAt = ?`,
        [memberId, relicId, off, filledSquads, closedAtSec, filledSquads, closedAtSec]
    );
}

async function upsertRelicPairFriend(
    connection: mysql.PoolConnection,
    relicId1: number,
    offcycle1: number,
    relicId2: number,
    offcycle2: number,
    closedAtSec: number,
    filled: number
): Promise<void> {
    let r1 = relicId1,
        o1 = offcycle1,
        r2 = relicId2,
        o2 = offcycle2;
    if (r1 > r2 || (r1 === r2 && o1 > o2)) {
        [r1, o1, r2, o2] = [r2, o2, r1, o1];
    }
    const filledSquads = filled === 1 ? 1 : 0;
    await connection.execute(
        `INSERT INTO ${TABLES.RELICPAIRFRIENDS} (RelicID1, RelicID2, Offcycle1, Offcycle2, SquadsTogether, FilledSquads, LastSquadAt) VALUES (?, ?, ?, ?, 1, ?, ?)
         ON DUPLICATE KEY UPDATE SquadsTogether = SquadsTogether + 1, FilledSquads = FilledSquads + ?, LastSquadAt = ?`,
        [r1, r2, o1, o2, filledSquads, closedAtSec, filledSquads, closedAtSec]
    );
}

/** Return squad IDs that are closed (Active=0, ClosedAt set) and not yet in reputation_backfill_processed. Optional maxClosedAtSec caps by ClosedAt (seconds) so totals match a cutoff. */
export async function getClosedSquadIdsNotYetProcessedForReputation(
    limit = 500,
    maxClosedAtSec?: number
): Promise<string[]> {
    const safeLimit = Math.max(1, Math.min(10000, Number(limit) || 500));
    const capClause = maxClosedAtSec != null && Number.isFinite(maxClosedAtSec) ? ` AND s.ClosedAt <= ${Math.floor(maxClosedAtSec)}` : '';
    const rows = await SelectQuery<{ SquadID: string }>(
        `SELECT s.SquadID FROM ${TABLES.SQUADS} s
         LEFT JOIN ${TABLES.REPUTATION_BACKFILL_PROCESSED} p ON s.SquadID = p.SquadID
         WHERE s.Active = 0 AND s.ClosedAt IS NOT NULL AND p.SquadID IS NULL${capClause}
         ORDER BY s.ClosedAt ASC
         LIMIT ${safeLimit}`
    );
    return rows.map((r) => r.SquadID);
}

/** Mark a squad as processed by the reputation backfill script. */
export async function markReputationBackfillProcessed(squadId: string): Promise<void> {
    await ModifyQuery(
        `INSERT INTO ${TABLES.REPUTATION_BACKFILL_PROCESSED} (SquadID, ProcessedAt) VALUES (?, ?)`,
        [squadId, Date.now()]
    );
}

/** Chunk size for bulk SELECT/INSERT in reputation backfill. Lower = less MySQL memory per query. */
const REP_BATCH_CHUNK = 250;

/** Process a full batch of squads in one transaction: aggregate reputation + friends, then bulk upsert. Much faster than per-squad transactions. */
export async function processReputationBackfillBatch(
    squads: ISquadRow[],
    squadUsers: ISquadUserRow[],
    squadRelics: ISquadRelicRow[],
    squadRefinements: ISquadRefinementRow[]
): Promise<void> {
    const withClosed = squads.filter((s) => s.ClosedAt != null).sort((a, b) => (a.ClosedAt ?? 0) - (b.ClosedAt ?? 0));
    if (withClosed.length === 0) return;

    const bySquadId = <T extends { SquadID: string }>(id: string, list: T[]): T[] => list.filter((r) => r.SquadID === id);
    const squadItems = withClosed.map((s) => ({
        s,
        users: bySquadId(s.SquadID, squadUsers),
        relics: bySquadId(s.SquadID, squadRelics),
        refs: bySquadId(s.SquadID, squadRefinements)
    }));

    const allMemberIds = new Set<number>();
    const allRelicIds = new Set<number>();
    const hostSignatures = new Map<string, number>();
    for (const { s, users, relics } of squadItems) {
        users.forEach((u) => allMemberIds.add(u.MemberID));
        relics.forEach((r) => allRelicIds.add(r.RelicID));
        const sig = buildHostSignatureFromRows(s, relics, squadRefinements.filter((r) => r.SquadID === s.SquadID));
        if (!hostSignatures.has(sig)) hostSignatures.set(sig, closedAtToSec(s.ClosedAt ?? 0));
    }
    const memberIds = [...allMemberIds];
    const relicIds = [...allRelicIds];
    const sigList = [...hostSignatures.keys()];

    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();

        const memberLastUpdate = new Map<number, number | null>();
        if (memberIds.length > 0) {
            for (let i = 0; i < memberIds.length; i += REP_BATCH_CHUNK) {
                const chunk = memberIds.slice(i, i + REP_BATCH_CHUNK);
                const ph = chunk.map(() => '?').join(',');
                const [rows] = await connection.execute<mysql.RowDataPacket[]>(
                    `SELECT MemberID, LastUpdate FROM ${TABLES.MEMBERREPUTATION} WHERE MemberID IN (${ph})`,
                    chunk
                );
                const arr = Array.isArray(rows) ? rows : [];
                arr.forEach((r) => memberLastUpdate.set(r.MemberID, r.LastUpdate as number | null));
            }
        }

        const memberDeltas = new Map<
            number,
            { total: number; filledSquads: number; day: number; week: number; month: number; threeMonth: number; sixMonth: number; year: number; allTime: number; lastUpdate: number }
        >();
        const cooldownSec = MEMBER_REPUTATION_COOLDOWN_SEC;
        for (const { s, users } of squadItems) {
            const closedAtSec = closedAtToSec(s.ClosedAt!);
            const filled = s.Filled ?? 0;
            const filledSquadsInc = filled === 1 ? 1 : 0;
            const inc = getRollingIncrements(closedAtSec);
            const memberSet = new Set(users.map((u) => u.MemberID));
            for (const memberId of memberSet) {
                let cur = memberDeltas.get(memberId);
                const lastUpdate = cur ? cur.lastUpdate : memberLastUpdate.get(memberId) ?? null;
                const applyFilled = filled === 1 && (lastUpdate == null || closedAtSec - lastUpdate >= cooldownSec);
                const dayInc = applyFilled ? inc.day : 0;
                const weekInc = applyFilled ? inc.week : 0;
                const monthInc = applyFilled ? inc.month : 0;
                const threeMonthInc = applyFilled ? inc.threeMonth : 0;
                const sixMonthInc = applyFilled ? inc.sixMonth : 0;
                const yearInc = applyFilled ? inc.year : 0;
                const allTimeInc = applyFilled ? 1 : 0;
                const newLastUpdate = applyFilled ? closedAtSec : (lastUpdate ?? closedAtSec);
                if (!cur) {
                    memberDeltas.set(memberId, {
                        total: 1,
                        filledSquads: filledSquadsInc,
                        day: dayInc,
                        week: weekInc,
                        month: monthInc,
                        threeMonth: threeMonthInc,
                        sixMonth: sixMonthInc,
                        year: yearInc,
                        allTime: allTimeInc,
                        lastUpdate: newLastUpdate
                    });
                } else {
                    cur.total += 1;
                    cur.filledSquads += filledSquadsInc;
                    cur.day += dayInc;
                    cur.week += weekInc;
                    cur.month += monthInc;
                    cur.threeMonth += threeMonthInc;
                    cur.sixMonth += sixMonthInc;
                    cur.year += yearInc;
                    cur.allTime += allTimeInc;
                    cur.lastUpdate = newLastUpdate;
                }
            }
        }

        const hostHashes = sigList.map((sig) => crypto.createHash('sha256').update(sig, 'utf8').digest());
        const hostIdBySig = new Map<string, number>();
        for (let i = 0; i < hostHashes.length; i += REP_BATCH_CHUNK) {
            const hashes = hostHashes.slice(i, i + REP_BATCH_CHUNK);
            const sigChunk = sigList.slice(i, i + REP_BATCH_CHUNK);
            const ph = hashes.map(() => '?').join(',');
            const [rows] = await connection.execute<mysql.RowDataPacket[]>(
                `SELECT HostID, SignatureHash FROM ${TABLES.HOSTS} WHERE SignatureHash IN (${ph})`,
                hashes
            );
            const arr = Array.isArray(rows) ? rows : [];
            const hashToSig = new Map<string, string>();
            hashes.forEach((h, k) => hashToSig.set(h.toString('hex'), sigChunk[k]));
            arr.forEach((r) => {
                const buf = r.SignatureHash as Buffer;
                const sig = buf ? hashToSig.get(buf.toString('hex')) : undefined;
                if (sig != null) hostIdBySig.set(sig, r.HostID);
            });
        }
        for (let j = 0; j < sigList.length; j++) {
            if (hostIdBySig.has(sigList[j])) continue;
            const hostId = await getOrCreateHost(connection, sigList[j], hostSignatures.get(sigList[j]) ?? 0);
            hostIdBySig.set(sigList[j], hostId);
        }
        const hostDeltas = new Map<
            number,
            { total: number; day: number; week: number; month: number; threeMonth: number; sixMonth: number; year: number; allTime: number; lastUpdate: number }
        >();
        for (const { s, relics, refs } of squadItems) {
            const closedAtSec = closedAtToSec(s.ClosedAt!);
            const filled = s.Filled ?? 0;
            const sig = buildHostSignatureFromRows(s, relics, refs);
            const hostId = hostIdBySig.get(sig);
            if (hostId == null) continue;
            const inc = getRollingIncrements(closedAtSec);
            const applyFilled = filled === 1;
            const dayInc = applyFilled ? inc.day : 0;
            const weekInc = applyFilled ? inc.week : 0;
            const monthInc = applyFilled ? inc.month : 0;
            const threeMonthInc = applyFilled ? inc.threeMonth : 0;
            const sixMonthInc = applyFilled ? inc.sixMonth : 0;
            const yearInc = applyFilled ? inc.year : 0;
            const allTimeInc = applyFilled ? 1 : 0;
            const cur = hostDeltas.get(hostId);
            if (!cur) {
                hostDeltas.set(hostId, {
                    total: 1,
                    day: dayInc,
                    week: weekInc,
                    month: monthInc,
                    threeMonth: threeMonthInc,
                    sixMonth: sixMonthInc,
                    year: yearInc,
                    allTime: allTimeInc,
                    lastUpdate: closedAtSec
                });
            } else {
                cur.total += 1;
                cur.day += dayInc;
                cur.week += weekInc;
                cur.month += monthInc;
                cur.threeMonth += threeMonthInc;
                cur.sixMonth += sixMonthInc;
                cur.year += yearInc;
                cur.allTime += allTimeInc;
                cur.lastUpdate = closedAtSec;
            }
        }

        const relicDeltas = new Map<
            number,
            { total: number; day: number; week: number; month: number; threeMonth: number; sixMonth: number; year: number; allTime: number; lastUpdate: number }
        >();
        for (const { s, relics } of squadItems) {
            const closedAtSec = closedAtToSec(s.ClosedAt!);
            const filled = s.Filled ?? 0;
            const inc = getRollingIncrements(closedAtSec);
            const applyFilled = filled === 1;
            const dayInc = applyFilled ? inc.day : 0;
            const weekInc = applyFilled ? inc.week : 0;
            const monthInc = applyFilled ? inc.month : 0;
            const threeMonthInc = applyFilled ? inc.threeMonth : 0;
            const sixMonthInc = applyFilled ? inc.sixMonth : 0;
            const yearInc = applyFilled ? inc.year : 0;
            const allTimeInc = applyFilled ? 1 : 0;
            for (const r of relics) {
                const relicId = r.RelicID;
                const cur = relicDeltas.get(relicId);
                if (!cur) {
                    relicDeltas.set(relicId, {
                        total: 1,
                        day: dayInc,
                        week: weekInc,
                        month: monthInc,
                        threeMonth: threeMonthInc,
                        sixMonth: sixMonthInc,
                        year: yearInc,
                        allTime: allTimeInc,
                        lastUpdate: closedAtSec
                    });
                } else {
                    cur.total += 1;
                    cur.day += dayInc;
                    cur.week += weekInc;
                    cur.month += monthInc;
                    cur.threeMonth += threeMonthInc;
                    cur.sixMonth += sixMonthInc;
                    cur.year += yearInc;
                    cur.allTime += allTimeInc;
                    cur.lastUpdate = closedAtSec;
                }
            }
        }

        const memberEntries = [...memberDeltas.entries()];
        for (let i = 0; i < memberEntries.length; i += REP_BATCH_CHUNK) {
            const chunk = memberEntries.slice(i, i + REP_BATCH_CHUNK);
            const values = chunk.map(([mid, d]) => [mid, d.day, d.week, d.month, d.threeMonth, d.sixMonth, d.year, d.allTime, d.filledSquads, d.total, d.lastUpdate]).flat();
            const placeholders = chunk.map(() => '(?,?,?,?,?,?,?,?,?,?,?)').join(',');
            await connection.execute(
                `INSERT INTO ${TABLES.MEMBERREPUTATION} (MemberID, Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, FilledSquads, TotalSquads, LastUpdate)
                 VALUES ${placeholders}
                 ON DUPLICATE KEY UPDATE TotalSquads = TotalSquads + VALUES(TotalSquads), FilledSquads = FilledSquads + VALUES(FilledSquads), Day = Day + VALUES(Day), Week = Week + VALUES(Week),
                 Month = Month + VALUES(Month), ThreeMonth = ThreeMonth + VALUES(ThreeMonth), SixMonth = SixMonth + VALUES(SixMonth), Year = Year + VALUES(Year),
                 AllTime = AllTime + VALUES(AllTime), LastUpdate = VALUES(LastUpdate)`,
                values
            );
        }

        const hostEntries = [...hostDeltas.entries()];
        for (let i = 0; i < hostEntries.length; i += REP_BATCH_CHUNK) {
            const chunk = hostEntries.slice(i, i + REP_BATCH_CHUNK);
            const values = chunk.map(([hid, d]) => [hid, d.day, d.week, d.month, d.threeMonth, d.sixMonth, d.year, d.allTime, d.total, d.lastUpdate]).flat();
            const placeholders = chunk.map(() => '(?,?,?,?,?,?,?,?,?,?)').join(',');
            await connection.execute(
                `INSERT INTO ${TABLES.HOSTREPUTATION} (HostID, Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, TotalSquads, LastUpdate)
                 VALUES ${placeholders}
                 ON DUPLICATE KEY UPDATE TotalSquads = TotalSquads + VALUES(TotalSquads), Day = Day + VALUES(Day), Week = Week + VALUES(Week),
                 Month = Month + VALUES(Month), ThreeMonth = ThreeMonth + VALUES(ThreeMonth), SixMonth = SixMonth + VALUES(SixMonth), Year = Year + VALUES(Year),
                 AllTime = AllTime + VALUES(AllTime), LastUpdate = VALUES(LastUpdate)`,
                values
            );
        }

        const relicEntries = [...relicDeltas.entries()];
        for (let i = 0; i < relicEntries.length; i += REP_BATCH_CHUNK) {
            const chunk = relicEntries.slice(i, i + REP_BATCH_CHUNK);
            const values = chunk.map(([rid, d]) => [rid, d.day, d.week, d.month, d.threeMonth, d.sixMonth, d.year, d.allTime, d.total, d.lastUpdate]).flat();
            const placeholders = chunk.map(() => '(?,?,?,?,?,?,?,?,?,?)').join(',');
            await connection.execute(
                `INSERT INTO ${TABLES.RELICREPUTATION} (RelicID, Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, TotalSquads, LastUpdate)
                 VALUES ${placeholders}
                 ON DUPLICATE KEY UPDATE TotalSquads = TotalSquads + VALUES(TotalSquads), Day = Day + VALUES(Day), Week = Week + VALUES(Week),
                 Month = Month + VALUES(Month), ThreeMonth = ThreeMonth + VALUES(ThreeMonth), SixMonth = SixMonth + VALUES(SixMonth), Year = Year + VALUES(Year),
                 AllTime = AllTime + VALUES(AllTime), LastUpdate = VALUES(LastUpdate)`,
                values
            );
        }

        const mfAgg = new Map<string, { count: number; filledCount: number; lastAt: number }>();
        const rfAgg = new Map<string, { count: number; filledCount: number; lastAt: number }>();
        const rpfAgg = new Map<string, { count: number; filledCount: number; lastAt: number }>();
        for (const { s, users, relics } of squadItems) {
            const closedAtSec = closedAtToSec(s.ClosedAt!);
            const filledInc = s.Filled === 1 ? 1 : 0;
            const mems = [...new Set(users.map((u) => u.MemberID))];
            const relList = relics.map((r) => ({ id: r.RelicID, off: r.Offcycle ?? 0 }));
            for (let a = 0; a < mems.length; a++) {
                for (let b = a + 1; b < mems.length; b++) {
                    const key = mems[a] < mems[b] ? `${mems[a]},${mems[b]}` : `${mems[b]},${mems[a]}`;
                    const cur = mfAgg.get(key);
                    if (!cur) mfAgg.set(key, { count: 1, filledCount: filledInc, lastAt: closedAtSec });
                    else {
                        cur.count += 1;
                        cur.filledCount += filledInc;
                        if (closedAtSec > cur.lastAt) cur.lastAt = closedAtSec;
                    }
                }
            }
            for (const m of mems) {
                for (const r of relList) {
                    const off = r.off === 1 ? 1 : 0;
                    const key = `${m},${r.id},${off}`;
                    const cur = rfAgg.get(key);
                    if (!cur) rfAgg.set(key, { count: 1, filledCount: filledInc, lastAt: closedAtSec });
                    else {
                        cur.count += 1;
                        cur.filledCount += filledInc;
                        if (closedAtSec > cur.lastAt) cur.lastAt = closedAtSec;
                    }
                }
            }
            for (let a = 0; a < relList.length; a++) {
                for (let b = a + 1; b < relList.length; b++) {
                    if (relList[a].id === relList[b].id) continue;
                    let r1 = relList[a].id,
                        o1 = relList[a].off,
                        r2 = relList[b].id,
                        o2 = relList[b].off;
                    if (r1 > r2 || (r1 === r2 && o1 > o2)) {
                        [r1, o1, r2, o2] = [r2, o2, r1, o1];
                    }
                    const key = `${r1},${o1},${r2},${o2}`;
                    const cur = rpfAgg.get(key);
                    if (!cur) rpfAgg.set(key, { count: 1, filledCount: filledInc, lastAt: closedAtSec });
                    else {
                        cur.count += 1;
                        cur.filledCount += filledInc;
                        if (closedAtSec > cur.lastAt) cur.lastAt = closedAtSec;
                    }
                }
            }
        }

        const mfEntries = [...mfAgg.entries()];
        for (let i = 0; i < mfEntries.length; i += REP_BATCH_CHUNK) {
            const chunk = mfEntries.slice(i, i + REP_BATCH_CHUNK);
            const values = chunk.flatMap(([k, v]) => {
                const [m1, m2] = k.split(',').map(Number);
                return [m1, m2, v.count, v.filledCount, v.lastAt];
            });
            const placeholders = chunk.map(() => '(?,?,?,?,?)').join(',');
            await connection.execute(
                `INSERT INTO ${TABLES.MEMBERFRIENDS} (MemberID1, MemberID2, SquadsTogether, FilledSquads, LastSquadAt) VALUES ${placeholders}
                 ON DUPLICATE KEY UPDATE SquadsTogether = SquadsTogether + VALUES(SquadsTogether), FilledSquads = FilledSquads + VALUES(FilledSquads), LastSquadAt = GREATEST(LastSquadAt, VALUES(LastSquadAt))`,
                values
            );
        }
        const rfEntries = [...rfAgg.entries()];
        for (let i = 0; i < rfEntries.length; i += REP_BATCH_CHUNK) {
            const chunk = rfEntries.slice(i, i + REP_BATCH_CHUNK);
            const values = chunk.flatMap(([k, v]) => {
                const parts = k.split(',');
                const m = Number(parts[0]);
                const rid = Number(parts[1]);
                const off = Number(parts[2]);
                return [m, rid, off, v.count, v.filledCount, v.lastAt];
            });
            const placeholders = chunk.map(() => '(?,?,?,?,?,?)').join(',');
            await connection.execute(
                `INSERT INTO ${TABLES.RELICFRIENDS} (MemberID, RelicID, Offcycle, SquadsTogether, FilledSquads, LastSquadAt) VALUES ${placeholders}
                 ON DUPLICATE KEY UPDATE SquadsTogether = SquadsTogether + VALUES(SquadsTogether), FilledSquads = FilledSquads + VALUES(FilledSquads), LastSquadAt = GREATEST(LastSquadAt, VALUES(LastSquadAt))`,
                values
            );
        }
        const rpfEntries = [...rpfAgg.entries()];
        for (let i = 0; i < rpfEntries.length; i += REP_BATCH_CHUNK) {
            const chunk = rpfEntries.slice(i, i + REP_BATCH_CHUNK);
            const values = chunk.flatMap(([k, v]) => {
                const [r1, o1, r2, o2] = k.split(',').map(Number);
                return [r1, r2, o1, o2, v.count, v.filledCount, v.lastAt];
            });
            const placeholders = chunk.map(() => '(?,?,?,?,?,?,?)').join(',');
            await connection.execute(
                `INSERT INTO ${TABLES.RELICPAIRFRIENDS} (RelicID1, RelicID2, Offcycle1, Offcycle2, SquadsTogether, FilledSquads, LastSquadAt) VALUES ${placeholders}
                 ON DUPLICATE KEY UPDATE SquadsTogether = SquadsTogether + VALUES(SquadsTogether), FilledSquads = FilledSquads + VALUES(FilledSquads), LastSquadAt = GREATEST(LastSquadAt, VALUES(LastSquadAt))`,
                values
            );
        }

        const processedAtSec = Math.floor(Date.now() / 1000);
        const processedValues = withClosed.flatMap((s) => [s.SquadID, processedAtSec]);
        for (let i = 0; i < processedValues.length; i += REP_BATCH_CHUNK * 2) {
            const chunk = processedValues.slice(i, i + REP_BATCH_CHUNK * 2);
            if (chunk.length === 0) continue;
            const placeholders = Array(chunk.length / 2)
                .fill('(?,?)')
                .join(',');
            await connection.execute(
                `INSERT INTO ${TABLES.REPUTATION_BACKFILL_PROCESSED} (SquadID, ProcessedAt) VALUES ${placeholders}`,
                chunk
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

/** Run reputation/friends updates for one squad using pre-loaded data. Used by backfill and by live close (updateReputationOnSquadClosed). Normalizes ClosedAt to seconds (handles legacy ms). */
export async function updateReputationOnSquadClosedFromData(
    s: ISquadRow,
    squadUsers: ISquadUserRow[],
    squadRelics: ISquadRelicRow[],
    squadRefinements: ISquadRefinementRow[]
): Promise<void> {
    if (s.ClosedAt == null) return;
    const closedAtSec = closedAtToSec(s.ClosedAt);
    const filled = s.Filled ?? 0;
    const memberIds = [...new Set(squadUsers.map((r) => r.MemberID))];
    const relicList = squadRelics.map((r) => ({ relicId: r.RelicID, offcycle: r.Offcycle ?? 0 }));

    const connection = await pool.getConnection();
    try {
        await connection.beginTransaction();

        const signatureStr = buildHostSignatureFromRows(s, squadRelics, squadRefinements);
        const hostId = await getOrCreateHost(connection, signatureStr, closedAtSec);

        for (const memberId of memberIds) {
            await upsertMemberReputation(connection, memberId, closedAtSec, filled);
        }
        await upsertHostReputation(connection, hostId, closedAtSec, filled);

        for (const { relicId } of relicList) {
            await upsertRelicReputation(connection, relicId, closedAtSec, filled);
        }

        for (let i = 0; i < memberIds.length; i++) {
            for (let j = i + 1; j < memberIds.length; j++) {
                await upsertMemberFriend(connection, memberIds[i], memberIds[j], closedAtSec, filled);
            }
        }
        for (const memberId of memberIds) {
            for (const { relicId, offcycle } of relicList) {
                await upsertRelicFriend(connection, memberId, relicId, offcycle, closedAtSec, filled);
            }
        }
        for (let i = 0; i < relicList.length; i++) {
            for (let j = i + 1; j < relicList.length; j++) {
                if (relicList[i].relicId === relicList[j].relicId) continue;
                await upsertRelicPairFriend(
                    connection,
                    relicList[i].relicId,
                    relicList[i].offcycle,
                    relicList[j].relicId,
                    relicList[j].offcycle,
                    closedAtSec,
                    filled
                );
            }
        }

        await connection.commit();
    } catch (err) {
        await connection.rollback();
        throw err;
    } finally {
        connection.release();
    }
}

/** Update member/host/relic reputation and friends tables when a squad has been closed. Call after setting ClosedAt/Filled/CloseReason. */
export async function updateReputationOnSquadClosed(squadId: string): Promise<void> {
    const { squad, squadUsers, squadRelics, squadRefinements } = await getSquadById(squadId);
    if (!squad || squad.length === 0 || squad[0].ClosedAt == null) {
        return;
    }
    await updateReputationOnSquadClosedFromData(squad[0], squadUsers, squadRelics, squadRefinements);
}

// --- Profile / stats GET helpers (ids + names) ---

export type ProfileSortField = 'filledSquads' | 'squadsTogether';

const PROFILE_SORT_COLUMNS: Record<ProfileSortField, string> = {
    filledSquads: 'FilledSquads',
    squadsTogether: 'SquadsTogether'
};

export function getProfileOrderBy(sort: string): ProfileSortField {
    const s = sort?.toLowerCase();
    return s === 'squadstogether' ? 'squadsTogether' : 'filledSquads';
}

/** Parse query param "all": all=1 or all=true -> show all data (filledOnly false). Omit or false -> filled only (default). Handles array (uses first element). */
export function parseFilledOnlyParam(all: unknown): boolean {
    if (Array.isArray(all)) all = all[0];
    if (all === undefined || all === null || all === false || all === '0' || all === 'false') return true;
    if (all === true || all === '1' || (typeof all === 'string' && all.toLowerCase() === 'true')) return false;
    return true;
}

export interface MemberProfileData {
    member: { id: number; name: string | null } | null;
    /** Stats for the requested period (or all-time when no date range). vrbReputation and missingReputation count as filled squads; added into AllTime, TotalSquads, FilledSquads, FilledSquadsCountedForRep. */
    reputation: Record<string, number> | null;
    /** When a date range is used, all-time totals from memberreputation plus vrbReputation and missingReputation (count as filled squads). Omitted when no range. */
    allTimeReputation?: Record<string, number> | null;
    topFriends: { id: number; name: string | null; squadsTogether: number; filledSquads: number }[];
    mostUsedRelicsOncycle: { id: number; name: string; era: string; squadsTogether: number; filledSquads: number }[];
    mostUsedRelicsOffcycle: { id: number; name: string; era: string; squadsTogether: number; filledSquads: number }[];
}

export async function getMemberProfileData(
    memberId: number,
    sortBy: ProfileSortField = 'filledSquads',
    filledOnly: boolean = true,
    dateRange: { fromSec: number; toSec: number } | null = null
): Promise<MemberProfileData> {
    const orderCol = PROFILE_SORT_COLUMNS[sortBy];
    if (dateRange) {
        return getMemberProfileDataInRange(memberId, sortBy, filledOnly, dateRange);
    }
    const filledFilter = filledOnly ? ' AND mf.FilledSquads > 0' : '';
    const missingFriendFilledFilter = filledOnly ? ' AND mf.FilledSquads > 0' : '';
    const rfFilledFilter = filledOnly ? ' AND rf.FilledSquads > 0' : '';
    const [memberRows, repRows, vrbRows, missingRepRows, friendRows, missingFriendRows, relicRowsOn, relicRowsOff] = await Promise.all([
        SelectQuery<IMemberRow>(`SELECT MemberID, MemberName FROM ${TABLES.MEMBERS} WHERE MemberID = ?`, [memberId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, TotalSquads, FilledSquads, LastUpdate FROM ${TABLES.MEMBERREPUTATION} WHERE MemberID = ?`,
            [memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(`SELECT reputation FROM ${TABLES.VRB_REPUTATION} WHERE id = ?`, [memberId]),
        SelectQuery<mysql.RowDataPacket>(`SELECT reputation FROM ${TABLES.MISSING_REPUTATION} WHERE id = ?`, [memberId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT mf.MemberID1, mf.MemberID2, mf.SquadsTogether, mf.FilledSquads, m1.MemberName AS Name1, m2.MemberName AS Name2
             FROM ${TABLES.MEMBERFRIENDS} mf
             JOIN ${TABLES.MEMBERS} m1 ON m1.MemberID = mf.MemberID1
             JOIN ${TABLES.MEMBERS} m2 ON m2.MemberID = mf.MemberID2
             WHERE (mf.MemberID1 = ? OR mf.MemberID2 = ?)${filledFilter}
             ORDER BY mf.${orderCol} DESC`,
            [memberId, memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT mf.MemberID1, mf.MemberID2, mf.SquadsTogether, mf.FilledSquads, m1.MemberName AS Name1, m2.MemberName AS Name2
             FROM ${TABLES.MISSING_FRIENDS} mf
             JOIN ${TABLES.MEMBERS} m1 ON m1.MemberID = mf.MemberID1
             JOIN ${TABLES.MEMBERS} m2 ON m2.MemberID = mf.MemberID2
             WHERE (mf.MemberID1 = ? OR mf.MemberID2 = ?)${missingFriendFilledFilter}`,
            [memberId, memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT rf.RelicID, rf.SquadsTogether, rf.FilledSquads, r.Era, r.Name
             FROM ${TABLES.RELICFRIENDS} rf
             JOIN ${TABLES.RELICS} r ON r.ID = rf.RelicID
             WHERE rf.MemberID = ? AND rf.Offcycle = 0${rfFilledFilter}
             ORDER BY rf.${orderCol} DESC`,
            [memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT rf.RelicID, rf.SquadsTogether, rf.FilledSquads, r.Era, r.Name
             FROM ${TABLES.RELICFRIENDS} rf
             JOIN ${TABLES.RELICS} r ON r.ID = rf.RelicID
             WHERE rf.MemberID = ? AND rf.Offcycle = 1${rfFilledFilter}
             ORDER BY rf.${orderCol} DESC`,
            [memberId]
        )
    ]);
    const member = memberRows[0];
    const rep = repRows[0];
    const vrbValue = Number((vrbRows[0] as { reputation?: number } | undefined)?.reputation ?? 0);
    const missingValue = Number((missingRepRows[0] as { reputation?: number } | undefined)?.reputation ?? 0);
    const legacyTotal = vrbValue + missingValue;
    const reputation = rep
        ? ({
            Day: Number(rep.Day),
            Week: Number(rep.Week),
            Month: Number(rep.Month),
            ThreeMonth: Number(rep.ThreeMonth),
            SixMonth: Number(rep.SixMonth),
            Year: Number(rep.Year),
            AllTime: Number(rep.AllTime) + legacyTotal,
            TotalSquads: Number(rep.TotalSquads) + legacyTotal,
            FilledSquads: Number(rep.FilledSquads) + legacyTotal,
            FilledSquadsCountedForRep: Number(rep.AllTime) + legacyTotal,
            LastUpdate: Number(rep.LastUpdate),
            vrbReputation: vrbValue,
            missingReputation: missingValue
        } as Record<string, number>)
        : (legacyTotal > 0
            ? ({ AllTime: legacyTotal, TotalSquads: legacyTotal, FilledSquads: legacyTotal, FilledSquadsCountedForRep: legacyTotal, vrbReputation: vrbValue, missingReputation: missingValue } as Record<string, number>)
            : null);
    const friendMap = new Map<string, { otherId: number; name: string | null; squadsTogether: number; filledSquads: number }>();
    const addFriend = (r: { MemberID1: number; MemberID2: number; SquadsTogether: number; FilledSquads?: number; Name1?: string; Name2?: string }) => {
        const otherId = r.MemberID1 === memberId ? r.MemberID2 : r.MemberID1;
        const name = r.MemberID1 === memberId ? r.Name2 : r.Name1;
        const key = `${Math.min(r.MemberID1, r.MemberID2)},${Math.max(r.MemberID1, r.MemberID2)}`;
        const existing = friendMap.get(key);
        const st = Number(r.SquadsTogether ?? 0);
        const fs = Number(r.FilledSquads ?? 0);
        if (existing) {
            existing.squadsTogether += st;
            existing.filledSquads += fs;
        } else {
            friendMap.set(key, { otherId, name: name ?? null, squadsTogether: st, filledSquads: fs });
        }
    };
    friendRows.forEach((r) => addFriend(r as { MemberID1: number; MemberID2: number; SquadsTogether: number; FilledSquads?: number; Name1?: string; Name2?: string }));
    missingFriendRows.forEach((r) => addFriend(r as { MemberID1: number; MemberID2: number; SquadsTogether: number; FilledSquads?: number; Name1?: string; Name2?: string }));
    const topFriends = [...friendMap.values()].map((v) => ({ id: v.otherId, name: v.name, squadsTogether: v.squadsTogether, filledSquads: v.filledSquads }))
        .sort((a, b) => (orderCol === 'FilledSquads' ? b.filledSquads - a.filledSquads : b.squadsTogether - a.squadsTogether));
    const mostUsedRelicsOncycle = relicRowsOn.map((r) => ({
        id: r.RelicID,
        name: r.Name,
        era: r.Era,
        squadsTogether: r.SquadsTogether,
        filledSquads: r.FilledSquads ?? 0
    }));
    const mostUsedRelicsOffcycle = relicRowsOff.map((r) => ({
        id: r.RelicID,
        name: r.Name,
        era: r.Era,
        squadsTogether: r.SquadsTogether,
        filledSquads: r.FilledSquads ?? 0
    }));
    return {
        member: member ? { id: member.MemberID!, name: member.MemberName ?? null } : null,
        reputation,
        topFriends,
        mostUsedRelicsOncycle,
        mostUsedRelicsOffcycle
    };
}

/** Normalize squads.ClosedAt to Unix seconds in SQL (DB may store sec or ms). Use in WHERE for date range. */
function closedAtSecExpr(alias: string): string {
    return `IF(${alias}.ClosedAt >= 10000000000, FLOOR(${alias}.ClosedAt/1000), ${alias}.ClosedAt)`;
}

/** Count how many filled squads "count" for reputation when applying 20-min cooldown (closedAtSecs must be sorted ascending). */
function countFilledWithCooldown(closedAtSecs: number[], cooldownSec: number): number {
    let count = 0;
    let lastCountedSec: number | null = null;
    for (const sec of closedAtSecs) {
        if (lastCountedSec == null || sec - lastCountedSec >= cooldownSec) {
            count += 1;
            lastCountedSec = sec;
        }
    }
    return count;
}

/** Member profile aggregated from squads where ClosedAt is in [fromSec, toSec]. */
async function getMemberProfileDataInRange(
    memberId: number,
    sortBy: ProfileSortField,
    filledOnly: boolean,
    range: { fromSec: number; toSec: number }
): Promise<MemberProfileData> {
    const { fromSec, toSec } = range;
    const orderCol = PROFILE_SORT_COLUMNS[sortBy];
    const closedRange = `${closedAtSecExpr('s')} BETWEEN ${fromSec} AND ${toSec}`;
    const [memberRows, repRangeRows, filledClosedRows, allTimeRepRows, vrbRows, missingRepRows, friendRows, missingFriendRows, relicOnRows, relicOffRows] = await Promise.all([
        SelectQuery<IMemberRow>(`SELECT MemberID, MemberName FROM ${TABLES.MEMBERS} WHERE MemberID = ?`, [memberId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT COUNT(*) AS TotalSquads, COALESCE(SUM(s.Filled), 0) AS FilledSquads
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID AND su.MemberID = ?
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})`,
            [memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT ${closedAtSecExpr('s')} AS closed_sec FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID AND su.MemberID = ?
             WHERE s.ClosedAt IS NOT NULL AND s.Filled = 1 AND (${closedRange})
             ORDER BY closed_sec`,
            [memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, TotalSquads, FilledSquads, LastUpdate FROM ${TABLES.MEMBERREPUTATION} WHERE MemberID = ?`,
            [memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(`SELECT reputation FROM ${TABLES.VRB_REPUTATION} WHERE id = ?`, [memberId]),
        SelectQuery<mysql.RowDataPacket>(`SELECT reputation FROM ${TABLES.MISSING_REPUTATION} WHERE id = ?`, [memberId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT su2.MemberID AS other_id, COUNT(*) AS SquadsTogether, COALESCE(SUM(s.Filled), 0) AS FilledSquads,
             m.MemberName
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADUSERS} su1 ON s.SquadID = su1.SquadID AND su1.MemberID = ?
             INNER JOIN ${TABLES.SQUADUSERS} su2 ON s.SquadID = su2.SquadID AND su2.MemberID != su1.MemberID
             INNER JOIN ${TABLES.MEMBERS} m ON m.MemberID = su2.MemberID
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY su2.MemberID, m.MemberName
             ${filledOnly ? 'HAVING FilledSquads > 0' : ''}
             ORDER BY ${orderCol === 'FilledSquads' ? 'FilledSquads' : 'SquadsTogether'} DESC`,
            [memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT mf.MemberID1, mf.MemberID2, mf.SquadsTogether, mf.FilledSquads, m1.MemberName AS Name1, m2.MemberName AS Name2
             FROM ${TABLES.MISSING_FRIENDS} mf
             JOIN ${TABLES.MEMBERS} m1 ON m1.MemberID = mf.MemberID1
             JOIN ${TABLES.MEMBERS} m2 ON m2.MemberID = mf.MemberID2
             WHERE (mf.MemberID1 = ? OR mf.MemberID2 = ?)${filledOnly ? ' AND mf.FilledSquads > 0' : ''}`,
            [memberId, memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT sr.RelicID, COUNT(*) AS SquadsTogether, COALESCE(SUM(s.Filled), 0) AS FilledSquads, r.Era, r.Name
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID AND su.MemberID = ?
             INNER JOIN ${TABLES.SQUADRELICS} sr ON s.SquadID = sr.SquadID AND sr.Offcycle = 0
             INNER JOIN ${TABLES.RELICS} r ON r.ID = sr.RelicID
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY sr.RelicID, r.Era, r.Name
             ${filledOnly ? 'HAVING FilledSquads > 0' : ''}
             ORDER BY ${orderCol === 'FilledSquads' ? 'FilledSquads' : 'SquadsTogether'} DESC`,
            [memberId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT sr.RelicID, COUNT(*) AS SquadsTogether, COALESCE(SUM(s.Filled), 0) AS FilledSquads, r.Era, r.Name
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID AND su.MemberID = ?
             INNER JOIN ${TABLES.SQUADRELICS} sr ON s.SquadID = sr.SquadID AND sr.Offcycle = 1
             INNER JOIN ${TABLES.RELICS} r ON r.ID = sr.RelicID
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY sr.RelicID, r.Era, r.Name
             ${filledOnly ? 'HAVING FilledSquads > 0' : ''}
             ORDER BY ${orderCol === 'FilledSquads' ? 'FilledSquads' : 'SquadsTogether'} DESC`,
            [memberId]
        )
    ]);
    const member = memberRows[0];
    const repRow = repRangeRows[0];
    const closedSecs = (filledClosedRows as { closed_sec: number }[]).map((r) => Number(r.closed_sec));
    const filledCountedForRep = countFilledWithCooldown(closedSecs, MEMBER_REPUTATION_COOLDOWN_SEC);
    const rep = repRow
        ? ({
            TotalSquads: Number(repRow.TotalSquads),
            FilledSquads: Number(repRow.FilledSquads),
            FilledSquadsCountedForRep: filledCountedForRep
        } as Record<string, number>)
        : null;
    const allTimeRow = allTimeRepRows[0];
    const vrbValueRange = Number((vrbRows[0] as { reputation?: number } | undefined)?.reputation ?? 0);
    const missingValueRange = Number((missingRepRows[0] as { reputation?: number } | undefined)?.reputation ?? 0);
    const legacyTotalRange = vrbValueRange + missingValueRange;
    const allTimeReputation = allTimeRow
        ? ({
            Day: Number(allTimeRow.Day),
            Week: Number(allTimeRow.Week),
            Month: Number(allTimeRow.Month),
            ThreeMonth: Number(allTimeRow.ThreeMonth),
            SixMonth: Number(allTimeRow.SixMonth),
            Year: Number(allTimeRow.Year),
            AllTime: Number(allTimeRow.AllTime) + legacyTotalRange,
            TotalSquads: Number(allTimeRow.TotalSquads) + legacyTotalRange,
            FilledSquads: Number(allTimeRow.FilledSquads) + legacyTotalRange,
            FilledSquadsCountedForRep: Number(allTimeRow.AllTime) + legacyTotalRange,
            LastUpdate: Number(allTimeRow.LastUpdate),
            vrbReputation: vrbValueRange,
            missingReputation: missingValueRange
        } as Record<string, number>)
        : (legacyTotalRange > 0
            ? ({ AllTime: legacyTotalRange, TotalSquads: legacyTotalRange, FilledSquads: legacyTotalRange, FilledSquadsCountedForRep: legacyTotalRange, vrbReputation: vrbValueRange, missingReputation: missingValueRange } as Record<string, number>)
            : null);
    const friendMapRange = new Map<number, { name: string | null; squadsTogether: number; filledSquads: number }>();
    friendRows.forEach((r) => {
        const row = r as { other_id: number; MemberName?: string; SquadsTogether: number; FilledSquads?: number };
        const id = row.other_id;
        const existing = friendMapRange.get(id);
        const st = Number(row.SquadsTogether ?? 0);
        const fs = Number(row.FilledSquads ?? 0);
        if (existing) {
            existing.squadsTogether += st;
            existing.filledSquads += fs;
        } else {
            friendMapRange.set(id, { name: row.MemberName ?? null, squadsTogether: st, filledSquads: fs });
        }
    });
    missingFriendRows.forEach((r) => {
        const row = r as { MemberID1: number; MemberID2: number; SquadsTogether: number; FilledSquads?: number; Name1?: string; Name2?: string };
        const otherId = row.MemberID1 === memberId ? row.MemberID2 : row.MemberID1;
        const name = row.MemberID1 === memberId ? row.Name2 : row.Name1;
        const existing = friendMapRange.get(otherId);
        const st = Number(row.SquadsTogether ?? 0);
        const fs = Number(row.FilledSquads ?? 0);
        if (existing) {
            existing.squadsTogether += st;
            existing.filledSquads += fs;
        } else {
            friendMapRange.set(otherId, { name: name ?? null, squadsTogether: st, filledSquads: fs });
        }
    });
    const topFriends = [...friendMapRange.entries()].map(([id, v]) => ({ id, name: v.name, squadsTogether: v.squadsTogether, filledSquads: v.filledSquads }))
        .sort((a, b) => (orderCol === 'FilledSquads' ? b.filledSquads - a.filledSquads : b.squadsTogether - a.squadsTogether));
    const mostUsedRelicsOncycle = relicOnRows.map((r) => ({
        id: r.RelicID,
        name: r.Name,
        era: r.Era,
        squadsTogether: Number(r.SquadsTogether),
        filledSquads: Number(r.FilledSquads ?? 0)
    }));
    const mostUsedRelicsOffcycle = relicOffRows.map((r) => ({
        id: r.RelicID,
        name: r.Name,
        era: r.Era,
        squadsTogether: Number(r.SquadsTogether),
        filledSquads: Number(r.FilledSquads ?? 0)
    }));
    return {
        member: member ? { id: member.MemberID!, name: member.MemberName ?? null } : null,
        reputation: rep,
        allTimeReputation,
        topFriends,
        mostUsedRelicsOncycle,
        mostUsedRelicsOffcycle
    };
}

export interface RelicProfileData {
    relic: { id: number; name: string; era: string } | null;
    reputation: Record<string, number> | null;
    /** Pairs where both relics were used on-cycle (Offcycle1=0, Offcycle2=0). */
    mostCommonPairsBothOn: { relicId: number; relicName: string; relicEra: string; squadsTogether: number; filledSquads: number }[];
    /** Pairs where first relic on-cycle, second off-cycle (Offcycle1=0, Offcycle2=1). */
    mostCommonPairsOnOff: { relicId: number; relicName: string; relicEra: string; squadsTogether: number; filledSquads: number }[];
    /** Pairs where first relic off-cycle, second on-cycle (Offcycle1=1, Offcycle2=0). */
    mostCommonPairsOffOn: { relicId: number; relicName: string; relicEra: string; squadsTogether: number; filledSquads: number }[];
    /** Pairs where both relics were used off-cycle (Offcycle1=1, Offcycle2=1). */
    mostCommonPairsBothOff: { relicId: number; relicName: string; relicEra: string; squadsTogether: number; filledSquads: number }[];
    topMembersOncycle: { id: number; name: string | null; squadsTogether: number; filledSquads: number }[];
    topMembersOffcycle: { id: number; name: string | null; squadsTogether: number; filledSquads: number }[];
    mostCommonHostsOncycle: { hostId: number; display: string; squadsCount: number }[];
    mostCommonHostsOffcycle: { hostId: number; display: string; squadsCount: number }[];
}

export async function getRelicProfileData(
    relicId: number,
    sortBy: ProfileSortField = 'filledSquads',
    filledOnly: boolean = true,
    dateRange: { fromSec: number; toSec: number } | null = null
): Promise<RelicProfileData> {
    if (dateRange) {
        return getRelicProfileDataInRange(relicId, sortBy, filledOnly, dateRange);
    }
    const orderCol = PROFILE_SORT_COLUMNS[sortBy];
    const pairFilledFilter = filledOnly ? ' AND rpf.FilledSquads > 0' : '';
    const rfFilledFilter = filledOnly ? ' AND rf.FilledSquads > 0' : '';
    const hostFilledJoin = filledOnly ? ` JOIN ${TABLES.SQUADS} s ON s.SquadID = sr.SquadID AND s.Filled = 1` : '';
    const [relicRows, repRows, pairRows, memberRowsOn, memberRowsOff, hostRowsOn, hostRowsOff] = await Promise.all([
        SelectQuery<IRelicRow>(`SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID = ?`, [relicId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT Day, Week, Month, ThreeMonth, SixMonth, Year, AllTime, TotalSquads, LastUpdate FROM ${TABLES.RELICREPUTATION} WHERE RelicID = ?`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT rpf.RelicID1, rpf.RelicID2, rpf.Offcycle1, rpf.Offcycle2, rpf.SquadsTogether, rpf.FilledSquads, r1.Era AS Era1, r1.Name AS Name1, r2.Era AS Era2, r2.Name AS Name2
             FROM ${TABLES.RELICPAIRFRIENDS} rpf
             JOIN ${TABLES.RELICS} r1 ON r1.ID = rpf.RelicID1
             JOIN ${TABLES.RELICS} r2 ON r2.ID = rpf.RelicID2
             WHERE (rpf.RelicID1 = ? OR rpf.RelicID2 = ?)${pairFilledFilter}
             ORDER BY rpf.${orderCol} DESC`,
            [relicId, relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT rf.MemberID, rf.SquadsTogether, rf.FilledSquads, m.MemberName
             FROM ${TABLES.RELICFRIENDS} rf
             JOIN ${TABLES.MEMBERS} m ON m.MemberID = rf.MemberID
             WHERE rf.RelicID = ? AND rf.Offcycle = 0${rfFilledFilter}
             ORDER BY rf.${orderCol} DESC`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT rf.MemberID, rf.SquadsTogether, rf.FilledSquads, m.MemberName
             FROM ${TABLES.RELICFRIENDS} rf
             JOIN ${TABLES.MEMBERS} m ON m.MemberID = rf.MemberID
             WHERE rf.RelicID = ? AND rf.Offcycle = 1${rfFilledFilter}
             ORDER BY rf.${orderCol} DESC`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT sh.HostID, h.Display, COUNT(*) AS squadsCount
             FROM ${TABLES.SQUADRELICS} sr${hostFilledJoin}
             JOIN squadhost sh ON sh.SquadID = sr.SquadID
             JOIN ${TABLES.HOSTS} h ON h.HostID = sh.HostID
             WHERE sr.RelicID = ? AND sr.Offcycle = 0
             GROUP BY sh.HostID, h.Display
             HAVING COUNT(*) > 0
             ORDER BY COUNT(*) DESC`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT sh.HostID, h.Display, COUNT(*) AS squadsCount
             FROM ${TABLES.SQUADRELICS} sr${hostFilledJoin}
             JOIN squadhost sh ON sh.SquadID = sr.SquadID
             JOIN ${TABLES.HOSTS} h ON h.HostID = sh.HostID
             WHERE sr.RelicID = ? AND sr.Offcycle = 1
             GROUP BY sh.HostID, h.Display
             HAVING COUNT(*) > 0
             ORDER BY COUNT(*) DESC`,
            [relicId]
        )
    ]);
    const relic = relicRows[0];
    const rep = repRows[0];
    const mapPair = (r: mysql.RowDataPacket) => {
        const isFirst = r.RelicID1 === relicId;
        return {
            relicId: isFirst ? r.RelicID2 : r.RelicID1,
            relicName: isFirst ? r.Name2 : r.Name1,
            relicEra: isFirst ? r.Era2 : r.Era1,
            squadsTogether: r.SquadsTogether,
            filledSquads: r.FilledSquads ?? 0
        };
    };
    const mostCommonPairsBothOn = pairRows.filter((r) => r.Offcycle1 === 0 && r.Offcycle2 === 0).map(mapPair);
    const mostCommonPairsOnOff = pairRows.filter((r) => r.Offcycle1 === 0 && r.Offcycle2 === 1).map(mapPair);
    const mostCommonPairsOffOn = pairRows.filter((r) => r.Offcycle1 === 1 && r.Offcycle2 === 0).map(mapPair);
    const mostCommonPairsBothOff = pairRows.filter((r) => r.Offcycle1 === 1 && r.Offcycle2 === 1).map(mapPair);
    const topMembersOncycle = memberRowsOn.map((r) => ({
        id: r.MemberID,
        name: r.MemberName ?? null,
        squadsTogether: r.SquadsTogether,
        filledSquads: r.FilledSquads ?? 0
    }));
    const topMembersOffcycle = memberRowsOff.map((r) => ({
        id: r.MemberID,
        name: r.MemberName ?? null,
        squadsTogether: r.SquadsTogether,
        filledSquads: r.FilledSquads ?? 0
    }));
    const allDisplays = hostRowsOn.map((r) => r.Display ?? '').concat(hostRowsOff.map((r) => r.Display ?? ''));
    const uniqueDisplays = [...new Set(allDisplays.filter(Boolean))];
    const displayToReadable = new Map<string, string>();
    await Promise.all(
        uniqueDisplays.map(async (d) => {
            const readable = await expandHostDisplay(d);
            displayToReadable.set(d, readable);
        })
    );
    const mostCommonHostsOncycle = hostRowsOn.map((r) => ({
        hostId: r.HostID,
        display: displayToReadable.get(r.Display ?? '') ?? r.Display ?? '',
        squadsCount: Number(r.squadsCount)
    }));
    const mostCommonHostsOffcycle = hostRowsOff.map((r) => ({
        hostId: r.HostID,
        display: displayToReadable.get(r.Display ?? '') ?? r.Display ?? '',
        squadsCount: Number(r.squadsCount)
    }));
    return {
        relic: relic ? { id: relic.ID, name: relic.Name, era: relic.Era } : null,
        reputation: rep ? { ...rep } as Record<string, number> : null,
        mostCommonPairsBothOn,
        mostCommonPairsOnOff,
        mostCommonPairsOffOn,
        mostCommonPairsBothOff,
        topMembersOncycle,
        topMembersOffcycle,
        mostCommonHostsOncycle,
        mostCommonHostsOffcycle
    };
}

/** Relic profile aggregated from squads where ClosedAt is in range. */
async function getRelicProfileDataInRange(
    relicId: number,
    sortBy: ProfileSortField,
    filledOnly: boolean,
    range: { fromSec: number; toSec: number }
): Promise<RelicProfileData> {
    const { fromSec, toSec } = range;
    const orderCol = PROFILE_SORT_COLUMNS[sortBy];
    const closedRange = `${closedAtSecExpr('s')} BETWEEN ${fromSec} AND ${toSec}`;
    const orderExpr = orderCol === 'FilledSquads' ? 'FilledSquads' : 'SquadsTogether';
    const [relicRows, repRows, pairRows, memberOnRows, memberOffRows, hostOnRows, hostOffRows] = await Promise.all([
        SelectQuery<IRelicRow>(`SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID = ?`, [relicId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT COUNT(*) AS TotalSquads, COALESCE(SUM(s.Filled), 0) AS FilledSquads
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADRELICS} sr ON s.SquadID = sr.SquadID AND sr.RelicID = ?
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT LEAST(sr1.RelicID, sr2.RelicID) AS RelicID1, GREATEST(sr1.RelicID, sr2.RelicID) AS RelicID2,
             IF(sr1.RelicID < sr2.RelicID, sr1.Offcycle, sr2.Offcycle) AS Offcycle1,
             IF(sr1.RelicID < sr2.RelicID, sr2.Offcycle, sr1.Offcycle) AS Offcycle2,
             COUNT(*) AS SquadsTogether, COALESCE(SUM(s.Filled), 0) AS FilledSquads,
             r1.Era AS Era1, r1.Name AS Name1, r2.Era AS Era2, r2.Name AS Name2
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADRELICS} sr1 ON s.SquadID = sr1.SquadID AND sr1.RelicID = ?
             INNER JOIN ${TABLES.SQUADRELICS} sr2 ON s.SquadID = sr2.SquadID AND sr2.RelicID != sr1.RelicID
             INNER JOIN ${TABLES.RELICS} r1 ON r1.ID = LEAST(sr1.RelicID, sr2.RelicID)
             INNER JOIN ${TABLES.RELICS} r2 ON r2.ID = GREATEST(sr1.RelicID, sr2.RelicID)
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY LEAST(sr1.RelicID, sr2.RelicID), GREATEST(sr1.RelicID, sr2.RelicID),
             IF(sr1.RelicID < sr2.RelicID, sr1.Offcycle, sr2.Offcycle),
             IF(sr1.RelicID < sr2.RelicID, sr2.Offcycle, sr1.Offcycle), r1.Era, r1.Name, r2.Era, r2.Name
             ${filledOnly ? 'HAVING FilledSquads > 0' : ''}
             ORDER BY ${orderExpr} DESC`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT su.MemberID, COUNT(*) AS SquadsTogether, COALESCE(SUM(s.Filled), 0) AS FilledSquads, m.MemberName
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADRELICS} sr ON s.SquadID = sr.SquadID AND sr.RelicID = ? AND sr.Offcycle = 0
             INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID
             INNER JOIN ${TABLES.MEMBERS} m ON m.MemberID = su.MemberID
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY su.MemberID, m.MemberName
             ${filledOnly ? 'HAVING FilledSquads > 0' : ''}
             ORDER BY ${orderExpr} DESC`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT su.MemberID, COUNT(*) AS SquadsTogether, COALESCE(SUM(s.Filled), 0) AS FilledSquads, m.MemberName
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADRELICS} sr ON s.SquadID = sr.SquadID AND sr.RelicID = ? AND sr.Offcycle = 1
             INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID
             INNER JOIN ${TABLES.MEMBERS} m ON m.MemberID = su.MemberID
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY su.MemberID, m.MemberName
             ${filledOnly ? 'HAVING FilledSquads > 0' : ''}
             ORDER BY ${orderExpr} DESC`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT sh.HostID, h.Display, COUNT(*) AS squadsCount
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADRELICS} sr ON s.SquadID = sr.SquadID AND sr.RelicID = ? AND sr.Offcycle = 0
             JOIN squadhost sh ON sh.SquadID = s.SquadID
             JOIN ${TABLES.HOSTS} h ON h.HostID = sh.HostID
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY sh.HostID, h.Display
             ORDER BY squadsCount DESC`,
            [relicId]
        ),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT sh.HostID, h.Display, COUNT(*) AS squadsCount
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADRELICS} sr ON s.SquadID = sr.SquadID AND sr.RelicID = ? AND sr.Offcycle = 1
             JOIN squadhost sh ON sh.SquadID = s.SquadID
             JOIN ${TABLES.HOSTS} h ON h.HostID = sh.HostID
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
             GROUP BY sh.HostID, h.Display
             ORDER BY squadsCount DESC`,
            [relicId]
        )
    ]);
    const relic = relicRows[0];
    const repRow = repRows[0];
    const rep = repRow
        ? ({ TotalSquads: Number(repRow.TotalSquads), FilledSquads: Number(repRow.FilledSquads) } as Record<string, number>)
        : null;
    const mapPair = (r: mysql.RowDataPacket) => {
        const isFirst = r.RelicID1 === relicId;
        return {
            relicId: isFirst ? r.RelicID2 : r.RelicID1,
            relicName: isFirst ? r.Name2 : r.Name1,
            relicEra: isFirst ? r.Era2 : r.Era1,
            squadsTogether: Number(r.SquadsTogether),
            filledSquads: Number(r.FilledSquads ?? 0)
        };
    };
    const mostCommonPairsBothOn = pairRows.filter((r) => r.Offcycle1 === 0 && r.Offcycle2 === 0).map(mapPair);
    const mostCommonPairsOnOff = pairRows.filter((r) => r.Offcycle1 === 0 && r.Offcycle2 === 1).map(mapPair);
    const mostCommonPairsOffOn = pairRows.filter((r) => r.Offcycle1 === 1 && r.Offcycle2 === 0).map(mapPair);
    const mostCommonPairsBothOff = pairRows.filter((r) => r.Offcycle1 === 1 && r.Offcycle2 === 1).map(mapPair);
    const topMembersOncycle = memberOnRows.map((r) => ({
        id: r.MemberID,
        name: r.MemberName ?? null,
        squadsTogether: Number(r.SquadsTogether),
        filledSquads: Number(r.FilledSquads ?? 0)
    }));
    const topMembersOffcycle = memberOffRows.map((r) => ({
        id: r.MemberID,
        name: r.MemberName ?? null,
        squadsTogether: Number(r.SquadsTogether),
        filledSquads: Number(r.FilledSquads ?? 0)
    }));
    const allDisplays = hostOnRows.map((r) => r.Display ?? '').concat(hostOffRows.map((r) => r.Display ?? ''));
    const uniqueDisplays = [...new Set(allDisplays.filter(Boolean))];
    const displayToReadable = new Map<string, string>();
    await Promise.all(
        uniqueDisplays.map(async (d) => {
            const readable = await expandHostDisplay(d);
            displayToReadable.set(d, readable);
        })
    );
    const mostCommonHostsOncycle = hostOnRows.map((r) => ({
        hostId: r.HostID,
        display: displayToReadable.get(r.Display ?? '') ?? r.Display ?? '',
        squadsCount: Number(r.squadsCount)
    }));
    const mostCommonHostsOffcycle = hostOffRows.map((r) => ({
        hostId: r.HostID,
        display: displayToReadable.get(r.Display ?? '') ?? r.Display ?? '',
        squadsCount: Number(r.squadsCount)
    }));
    return {
        relic: relic ? { id: relic.ID, name: relic.Name, era: relic.Era } : null,
        reputation: rep,
        mostCommonPairsBothOn,
        mostCommonPairsOnOff,
        mostCommonPairsOffOn,
        mostCommonPairsBothOff,
        topMembersOncycle,
        topMembersOffcycle,
        mostCommonHostsOncycle,
        mostCommonHostsOffcycle
    };
}

export interface HostProfileData {
    host: { id: number; display: string; style: string | null } | null;
    reputation: Record<string, number> | null;
    formatted: { style: string | null; relics: { id: number; name: string; offcycle: number }[]; refinements: { id: number; name: string; offcycle: number }[] };
}

export async function getHostProfileData(
    hostId: number,
    filledOnly: boolean = true,
    dateRange: { fromSec: number; toSec: number } | null = null
): Promise<HostProfileData> {
    if (dateRange) {
        return getHostProfileDataInRange(hostId, filledOnly, dateRange);
    }
    const [hostRows, repRows] = await Promise.all([
        SelectQuery<mysql.RowDataPacket>(`SELECT HostID, Display, Style FROM ${TABLES.HOSTS} WHERE HostID = ?`, [hostId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT * FROM ${TABLES.HOSTREPUTATION} WHERE HostID = ?`,
            [hostId]
        )
    ]);
    const host = hostRows[0];
    const rep = repRows[0];
    if (!host) {
        return { host: null, reputation: rep ? { ...rep } as Record<string, number> : null, formatted: { style: null, relics: [], refinements: [] } };
    }
    if (filledOnly) {
        const filled = Number((rep as Record<string, number>)?.['AllTime']) || 0;
        if (filled === 0) {
            return { host: null, reputation: rep ? { ...rep } as Record<string, number> : null, formatted: { style: host.Style ?? null, relics: [], refinements: [] } };
        }
    }
    const display = host?.Display ?? '';
    const style = host?.Style ?? null;
    const parsed = parseHostDisplay(display);
    const relics: { id: number; name: string; offcycle: number }[] = [];
    const refinements: { id: number; name: string; offcycle: number }[] = [];
    const relicMap = new Map<number, { era: string; name: string }>();
    const refMap = new Map<number, string>();
    if (parsed) {
        for (const [id, off] of parsed.relicSegs) {
            const r = await SelectQuery<IRelicRow>(`SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID = ?`, [id]);
            const row = r[0];
            if (row) {
                relicMap.set(id, { era: row.Era ?? '', name: row.Name ?? '' });
                relics.push({ id, name: `${row.Era ?? ''} ${row.Name ?? ''}`.trim(), offcycle: off });
            } else {
                relics.push({ id, name: String(id), offcycle: off });
            }
        }
        for (const [id, off] of parsed.refSegs) {
            const ref = await SelectQuery<IRefinementRow>(`SELECT ID, Name FROM ${TABLES.REFINEMENT} WHERE ID = ?`, [id]);
            const name = ref[0]?.Name ?? String(id);
            refMap.set(id, name);
            refinements.push({ id, name, offcycle: off });
        }
    }
    const displayReadable = formatHostDisplayReadable(parsed, relicMap, refMap) || display;
    return {
        host: host ? { id: host.HostID, display: displayReadable, style } : null,
        reputation: rep ? { ...rep } as Record<string, number> : null,
        formatted: { style, relics, refinements }
    };
}

/** Host profile for squads in date range only. */
async function getHostProfileDataInRange(
    hostId: number,
    filledOnly: boolean,
    range: { fromSec: number; toSec: number }
): Promise<HostProfileData> {
    const { fromSec, toSec } = range;
    const closedRange = `${closedAtSecExpr('s')} BETWEEN ${fromSec} AND ${toSec}`;
    const [hostRows, rangeRows] = await Promise.all([
        SelectQuery<mysql.RowDataPacket>(`SELECT HostID, Display, Style FROM ${TABLES.HOSTS} WHERE HostID = ?`, [hostId]),
        SelectQuery<mysql.RowDataPacket>(
            `SELECT COUNT(*) AS TotalSquads, COALESCE(SUM(s.Filled), 0) AS FilledSquads
             FROM ${TABLES.SQUADS} s
             INNER JOIN squadhost sh ON s.SquadID = sh.SquadID AND sh.HostID = ?
             WHERE s.ClosedAt IS NOT NULL AND (${closedRange})`,
            [hostId]
        )
    ]);
    const host = hostRows[0];
    if (!host) {
        return { host: null, reputation: null, formatted: { style: null, relics: [], refinements: [] } };
    }
    const r = rangeRows[0];
    const total = Number(r?.TotalSquads) || 0;
    const filled = Number(r?.FilledSquads) || 0;
    if (filledOnly && filled === 0) {
        return { host: null, reputation: { TotalSquads: total, FilledSquads: filled }, formatted: { style: host.Style ?? null, relics: [], refinements: [] } };
    }
    const display = host.Display ?? '';
    const style = host.Style ?? null;
    const parsed = parseHostDisplay(display);
    const relics: { id: number; name: string; offcycle: number }[] = [];
    const refinements: { id: number; name: string; offcycle: number }[] = [];
    const relicMap = new Map<number, { era: string; name: string }>();
    const refMap = new Map<number, string>();
    if (parsed) {
        for (const [id, off] of parsed.relicSegs) {
            const rel = await SelectQuery<IRelicRow>(`SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID = ?`, [id]);
            const row = rel[0];
            if (row) {
                relicMap.set(id, { era: row.Era ?? '', name: row.Name ?? '' });
                relics.push({ id, name: `${row.Era ?? ''} ${row.Name ?? ''}`.trim(), offcycle: off });
            } else {
                relics.push({ id, name: String(id), offcycle: off });
            }
        }
        for (const [id, off] of parsed.refSegs) {
            const ref = await SelectQuery<IRefinementRow>(`SELECT ID, Name FROM ${TABLES.REFINEMENT} WHERE ID = ?`, [id]);
            refinements.push({ id, name: ref[0]?.Name ?? String(id), offcycle: off });
            refMap.set(id, ref[0]?.Name ?? '');
        }
    }
    const displayReadable = formatHostDisplayReadable(parsed, relicMap, refMap) || display;
    return {
        host: { id: host.HostID, display: displayReadable, style },
        reputation: { TotalSquads: total, FilledSquads: filled },
        formatted: { style, relics, refinements }
    };
}

export type HostProfileSortField = 'filledSquads' | 'totalSquads';

const HOST_PROFILE_SORT_COLUMNS: Record<HostProfileSortField, string> = {
    filledSquads: 'FilledSquads',
    totalSquads: 'TotalSquads'
};

export function getHostProfileOrderBy(sort: string): HostProfileSortField {
    const s = sort?.toLowerCase();
    return s === 'totalsquads' ? 'totalSquads' : 'filledSquads';
}

export async function getAllHostsProfileData(
    sortBy: HostProfileSortField = 'filledSquads',
    filledOnly: boolean = true,
    dateRange: { fromSec: number; toSec: number } | null = null
): Promise<HostProfileData[]> {
    if (dateRange) {
        return getAllHostsProfileDataInRange(sortBy, filledOnly, dateRange);
    }
    const [hostRows, repRows] = await Promise.all([
        SelectQuery<mysql.RowDataPacket>(`SELECT HostID, Display, Style FROM ${TABLES.HOSTS}`),
        SelectQuery<mysql.RowDataPacket>(`SELECT * FROM ${TABLES.HOSTREPUTATION}`)
    ]);
    const repByHostId = new Map<number, Record<string, number>>();
    repRows.forEach((r) => repByHostId.set(r.HostID, { ...r } as Record<string, number>));
    const allRelicIds = new Set<number>();
    const allRefinementIds = new Set<number>();
    const hostParsed: { hostId: number; parsed: ParsedHostDisplay | null; style: string | null; relicSegs: [number, number][]; refSegs: [number, number][] }[] = [];
    for (const h of hostRows) {
        const display = h.Display ?? '';
        const style = h.Style ?? null;
        const parsed = parseHostDisplay(display);
        const relicSegs = parsed?.relicSegs ?? [];
        const refSegs = parsed?.refSegs ?? [];
        relicSegs.forEach(([id]) => allRelicIds.add(id));
        refSegs.forEach(([id]) => allRefinementIds.add(id));
        hostParsed.push({ hostId: h.HostID, parsed, style, relicSegs, refSegs });
    }
    const relicIds = [...allRelicIds];
    const refIds = [...allRefinementIds];
    const relicMap = new Map<number, { era: string; name: string }>();
    const refMap = new Map<number, string>();
    if (relicIds.length > 0) {
        const ph = relicIds.map(() => '?').join(',');
        const relics = await SelectQuery<IRelicRow>(`SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID IN (${ph})`, relicIds);
        relics.forEach((r) => relicMap.set(r.ID, { era: r.Era, name: r.Name }));
    }
    if (refIds.length > 0) {
        const ph = refIds.map(() => '?').join(',');
        const refs = await SelectQuery<IRefinementRow>(`SELECT ID, Name FROM ${TABLES.REFINEMENT} WHERE ID IN (${ph})`, refIds);
        refs.forEach((r) => refMap.set(r.ID, r.Name ?? ''));
    }
    const sortCol = HOST_PROFILE_SORT_COLUMNS[sortBy];
    const results = hostParsed.map(({ hostId, parsed, style, relicSegs, refSegs }) => {
        const displayReadable = formatHostDisplayReadable(parsed, relicMap, refMap);
        return {
            host: { id: hostId, display: displayReadable, style },
            reputation: repByHostId.get(hostId) ?? null,
            formatted: {
                style,
                relics: relicSegs.map(([id, off]) => ({
                    id,
                    name: relicMap.has(id) ? `${relicMap.get(id)!.era} ${relicMap.get(id)!.name}` : String(id),
                    offcycle: off
                })),
                refinements: refSegs.map(([id, off]) => ({
                    id,
                    name: refMap.get(id) ?? String(id),
                    offcycle: off
                }))
            }
        };
    });
    let output = results;
    if (filledOnly) {
        output = results.filter((row) => {
            const rep = (row.reputation as Record<string, number> | null) ?? {};
            const allTime = Number(rep.AllTime) || 0;
            return allTime > 0;
        });
    }
    output.sort((a, b) => {
        const rep = (x: HostProfileData) => (x.reputation as Record<string, number> | null) ?? {};
        const val = (r: Record<string, number>) =>
            sortCol === 'FilledSquads'
                ? Number(r.FilledSquads ?? r.TotalSquads) || 0
                : Number(r[sortCol]) || 0;
        return val(rep(b)) - val(rep(a));
    });
    return output;
}

/** Host list aggregated from squads where ClosedAt is in range. */
async function getAllHostsProfileDataInRange(
    sortBy: HostProfileSortField,
    filledOnly: boolean,
    range: { fromSec: number; toSec: number }
): Promise<HostProfileData[]> {
    const { fromSec, toSec } = range;
    const closedRange = `${closedAtSecExpr('s')} BETWEEN ${fromSec} AND ${toSec}`;
    const rangeRows = await SelectQuery<mysql.RowDataPacket>(
        `SELECT sh.HostID, COUNT(*) AS TotalSquads, COALESCE(SUM(s.Filled), 0) AS FilledSquads
         FROM ${TABLES.SQUADS} s
         INNER JOIN squadhost sh ON s.SquadID = sh.SquadID
         WHERE s.ClosedAt IS NOT NULL AND (${closedRange})
         GROUP BY sh.HostID`
    );
    let filtered = rangeRows;
    if (filledOnly) {
        filtered = rangeRows.filter((r) => Number(r.FilledSquads) > 0);
    }
    if (filtered.length === 0) return [];
    const hostIds = filtered.map((r) => r.HostID);
    const ph = hostIds.map(() => '?').join(',');
    const hostRows = await SelectQuery<mysql.RowDataPacket>(`SELECT HostID, Display, Style FROM ${TABLES.HOSTS} WHERE HostID IN (${ph})`, hostIds);
    const repByHostId = new Map<number, Record<string, number>>();
    filtered.forEach((r) => repByHostId.set(r.HostID, { TotalSquads: Number(r.TotalSquads), FilledSquads: Number(r.FilledSquads) }));
    const allRelicIds = new Set<number>();
    const allRefinementIds = new Set<number>();
    const hostParsed: { hostId: number; parsed: ParsedHostDisplay | null; style: string | null; relicSegs: [number, number][]; refSegs: [number, number][] }[] = [];
    for (const h of hostRows) {
        const parsed = parseHostDisplay(h.Display ?? '');
        const relicSegs = parsed?.relicSegs ?? [];
        const refSegs = parsed?.refSegs ?? [];
        relicSegs.forEach(([id]) => allRelicIds.add(id));
        refSegs.forEach(([id]) => allRefinementIds.add(id));
        hostParsed.push({ hostId: h.HostID, parsed, style: h.Style ?? null, relicSegs, refSegs });
    }
    const relicMap = new Map<number, { era: string; name: string }>();
    const refMap = new Map<number, string>();
    const relicIds = [...allRelicIds];
    const refIds = [...allRefinementIds];
    if (relicIds.length > 0) {
        const relPh = relicIds.map(() => '?').join(',');
        const relics = await SelectQuery<IRelicRow>(`SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID IN (${relPh})`, relicIds);
        relics.forEach((r) => relicMap.set(r.ID, { era: r.Era ?? '', name: r.Name ?? '' }));
    }
    if (refIds.length > 0) {
        const refPh = refIds.map(() => '?').join(',');
        const refs = await SelectQuery<IRefinementRow>(`SELECT ID, Name FROM ${TABLES.REFINEMENT} WHERE ID IN (${refPh})`, refIds);
        refs.forEach((r) => refMap.set(r.ID, r.Name ?? ''));
    }
    const sortCol = HOST_PROFILE_SORT_COLUMNS[sortBy];
    const results = hostParsed.map(({ hostId, parsed, style, relicSegs, refSegs }) => ({
        host: { id: hostId, display: formatHostDisplayReadable(parsed, relicMap, refMap), style },
        reputation: repByHostId.get(hostId) ?? null,
        formatted: {
            style,
            relics: relicSegs.map(([id, off]) => ({
                id,
                name: relicMap.has(id) ? `${relicMap.get(id)!.era} ${relicMap.get(id)!.name}` : String(id),
                offcycle: off
            })),
            refinements: refSegs.map(([id, off]) => ({
                id,
                name: refMap.get(id) ?? String(id),
                offcycle: off
            }))
        }
    }));
    results.sort((a, b) => {
        const rep = (x: HostProfileData) => (x.reputation as Record<string, number> | null) ?? {};
        const val = (r: Record<string, number>) =>
            sortCol === 'FilledSquads' ? Number(r.FilledSquads ?? r.TotalSquads) || 0 : Number(r[sortCol]) || 0;
        return val(rep(b)) - val(rep(a));
    });
    return results;
}
