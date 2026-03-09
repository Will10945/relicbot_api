/**
 * Export closed, filled squads to CSV for Excel.
 *
 * Columns: created date, create time, closed date, closed time, duration,
 * host message (relics style refinements), squad member 1, 2, 3, 4, ... (guests
 * listed immediately after the member who added them).
 *
 * Usage: npx ts-node src/scripts/export-squads-csv.ts [output.csv]
 *   Default output: stdout. Pass a path to write to a file.
 * Env: MYSQL_* (same as API).
 */

import * as fs from 'fs';
import {
    SelectQuery,
    getSquadsByIds
} from '../database/database';
import type ISquadRow from '../entities/db.squads';
import type { ISquadUserRow, ISquadRelicRow, ISquadRefinementRow } from '../entities/db.squads';
import type IRelicRow from '../entities/db.relics';
import type IRefinementRow from '../entities/db.refinement';
import TABLES from '../entities/constants';

const BATCH_SIZE = 10000;
const PARTICIPANT_COLUMNS = 4;

function toMs(secOrMs: number): number {
    return secOrMs > 1e10 ? secOrMs : secOrMs * 1000;
}

function formatDate(ms: number): string {
    return new Date(ms).toISOString().slice(0, 10);
}

function formatTime(ms: number): string {
    return new Date(ms).toISOString().slice(11, 19);
}

function formatDuration(createdMs: number, closedMs: number): string {
    const totalMs = closedMs - createdMs;
    if (totalMs < 0) return '0m';
    const totalMins = Math.floor(totalMs / 60000);
    const hours = Math.floor(totalMins / 60);
    const mins = totalMins % 60;
    if (hours > 0) return `${hours}h ${mins}m`;
    return `${mins}m`;
}

function escapeCsvField(value: string): string {
    if (value === '') return '';
    const s = String(value);
    if (/[,"\n\r]/.test(s)) {
        return '"' + s.replace(/"/g, '""') + '"';
    }
    return s;
}

async function main(): Promise<void> {
    const outPath = process.argv[2];
    const write = outPath
        ? (line: string) => { fs.appendFileSync(outPath, line, 'utf8'); }
        : (line: string) => { process.stdout.write(line); };

    const numColumns = 6 + PARTICIPANT_COLUMNS;
    const participantHeaders = Array.from({ length: PARTICIPANT_COLUMNS }, (_, i) => `squad member ${i + 1}`);
    const header = [
        'created date',
        'create time',
        'closed date',
        'closed time',
        'duration',
        'host message (relics style refinements)',
        ...participantHeaders
    ].map(escapeCsvField).join(',');

    const memberNames = new Map<number, string>();
    const relicLabels = new Map<number, string>();
    const refinementNamesMap = new Map<number, string>();

    function buildHostMessage(s: ISquadRow, relics: ISquadRelicRow[], refs: ISquadRefinementRow[]): string {
        const parts: string[] = [];
        const relicStrs = relics
            .sort((a, b) => a.RelicID - b.RelicID || (a.Offcycle ?? 0) - (b.Offcycle ?? 0))
            .map((r) => relicLabels.get(r.RelicID) ?? '');
        if (relicStrs.length) parts.push(relicStrs.join(', '));
        if (s.Style) parts.push(s.Style);
        const refStrs = refs
            .sort((a, b) => a.RefinementID - b.RefinementID || (a.Offcycle ?? 0) - (b.Offcycle ?? 0))
            .map((r) => refinementNamesMap.get(r.RefinementID) ?? '')
            .filter(Boolean);
        if (refStrs.length) parts.push(refStrs.join(', '));
        const base = parts.filter(Boolean).join(' | ');
        if (s.UserMsg && s.UserMsg.trim()) return base ? `${base} | ${s.UserMsg.trim()}` : s.UserMsg.trim();
        return base;
    }

    function buildParticipants(users: ISquadUserRow[]): string[] {
        const out: string[] = [];
        const sorted = [...users].sort((a, b) => a.MemberID - b.MemberID);
        for (const u of sorted) {
            out.push(memberNames.get(u.MemberID) ?? '');
            const anon = u.AnonymousUsers ?? 0;
            for (let i = 0; i < anon; i++) out.push('guest');
        }
        return out;
    }

    console.error('Writing CSV header...');
    write(header + '\n');

    let offset = 0;
    let totalWritten = 0;

    while (true) {
        console.error(`Loading closed, filled squads (offset ${offset}, limit ${BATCH_SIZE})...`);
        const squadsRows = await SelectQuery<ISquadRow>(
            `SELECT SquadID, CreatedAt, ClosedAt, Host, Style, Era, UserMsg FROM ${TABLES.SQUADS}
             WHERE ClosedAt IS NOT NULL AND Filled = 1
             ORDER BY ClosedAt ASC
             LIMIT ${BATCH_SIZE} OFFSET ${offset}`
        );
        if (squadsRows.length === 0) {
            if (offset === 0) console.error('No closed, filled squads found.');
            break;
        }

        console.error(`Loaded ${squadsRows.length} squads. Fetching users, relics, refinements...`);
        const squadIds = squadsRows.map((r) => r.SquadID);
        const { squadUsers, squadRelics, squadRefinements } = await getSquadsByIds(squadIds, { skipPosts: true });

        const usersBySquad = new Map<string, ISquadUserRow[]>();
        for (const u of squadUsers) {
            const list = usersBySquad.get(u.SquadID) ?? [];
            list.push(u);
            usersBySquad.set(u.SquadID, list);
        }
        const relicsBySquad = new Map<string, ISquadRelicRow[]>();
        for (const r of squadRelics) {
            const list = relicsBySquad.get(r.SquadID) ?? [];
            list.push(r);
            relicsBySquad.set(r.SquadID, list);
        }
        const refinementsBySquad = new Map<string, ISquadRefinementRow[]>();
        for (const r of squadRefinements) {
            const list = refinementsBySquad.get(r.SquadID) ?? [];
            list.push(r);
            refinementsBySquad.set(r.SquadID, list);
        }

        const memberIds = new Set<number>();
        for (const s of squadsRows) {
            if (s.Host != null) memberIds.add(s.Host);
        }
        for (const u of squadUsers) {
            memberIds.add(u.MemberID);
        }
        const relicIds = new Set(squadRelics.map((r) => r.RelicID));
        const refinementIds = new Set(squadRefinements.map((r) => r.RefinementID));

        const needMembers = Array.from(memberIds).filter((id) => !memberNames.has(id));
        const needRelics = Array.from(relicIds).filter((id) => !relicLabels.has(id));
        const needRefinements = Array.from(refinementIds).filter((id) => !refinementNamesMap.has(id));

        if (needMembers.length > 0 || needRelics.length > 0 || needRefinements.length > 0) {
            const [memberRows, relicRows, refinementRows] = await Promise.all([
                needMembers.length > 0
                    ? SelectQuery<{ MemberID: number; MemberName: string | null }>(
                        `SELECT MemberID, MemberName FROM ${TABLES.MEMBERS} WHERE MemberID IN (${needMembers.map(() => '?').join(',')})`,
                        needMembers
                    )
                    : Promise.resolve([]),
                needRelics.length > 0
                    ? SelectQuery<IRelicRow>(
                        `SELECT ID, Era, Name FROM ${TABLES.RELICS} WHERE ID IN (${needRelics.map(() => '?').join(',')})`,
                        needRelics
                    )
                    : Promise.resolve([]),
                needRefinements.length > 0
                    ? SelectQuery<IRefinementRow>(
                        `SELECT ID, Name FROM ${TABLES.REFINEMENT} WHERE ID IN (${needRefinements.map(() => '?').join(',')})`,
                        needRefinements
                    )
                    : Promise.resolve([])
            ]);
            for (const m of memberRows) {
                memberNames.set(m.MemberID, m.MemberName ?? '');
            }
            for (const r of relicRows) {
                relicLabels.set(r.ID, `${r.Era ?? ''} ${r.Name ?? ''}`.trim());
            }
            for (const r of refinementRows) {
                refinementNamesMap.set(r.ID, r.Name ?? '');
            }
        }

        for (const s of squadsRows) {
            const createdMs = toMs(s.CreatedAt ?? 0);
            const closedMs = toMs(s.ClosedAt ?? 0);
            const users = usersBySquad.get(s.SquadID) ?? [];
            const relics = relicsBySquad.get(s.SquadID) ?? [];
            const refs = refinementsBySquad.get(s.SquadID) ?? [];

            const participants = buildParticipants(users);
            const participantsPadded = participants.slice(0, PARTICIPANT_COLUMNS);
            while (participantsPadded.length < PARTICIPANT_COLUMNS) participantsPadded.push('');
            const rowOut = [
                formatDate(createdMs),
                formatTime(createdMs),
                formatDate(closedMs),
                formatTime(closedMs),
                formatDuration(createdMs, closedMs),
                buildHostMessage(s, relics, refs),
                ...participantsPadded
            ];
            write(rowOut.map(escapeCsvField).join(',') + '\n');
        }

        totalWritten += squadsRows.length;
        console.error(`Wrote ${squadsRows.length} rows (total: ${totalWritten}).`);

        if (squadsRows.length < BATCH_SIZE) break;
        offset += BATCH_SIZE;
    }

    if (totalWritten > 0) {
        if (outPath) {
            console.error(`Done. Wrote ${totalWritten} rows to ${outPath}`);
        } else {
            console.error(`Done. Wrote ${totalWritten} rows to stdout`);
        }
    } else if (outPath) {
        fs.writeFileSync(outPath, '', 'utf8');
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
