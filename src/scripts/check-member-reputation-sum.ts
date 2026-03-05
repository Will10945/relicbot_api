/**
 * Check that each member's cumulative daily reputation (from squads) matches
 * their stored memberreputation totals.
 *
 * It compares, per member:
 * - Sum of daily total squads      vs memberreputation.TotalSquads
 * - Sum of daily filled squads     vs memberreputation.FilledSquads
 * - Sum of daily filled squads     vs (AllTime + vrb_reputation + missing_reputation)
 *
 * Usage:
 *   npx ts-node src/scripts/check-member-reputation-sum.ts
 */

import TABLES from '../entities/constants';
import { SelectQuery } from '../database/database';

const MEMBER_REPUTATION_COOLDOWN_SEC = 20 * 60;

/** Count how many filled squads "count" for reputation when applying cooldown. */
function countFilledWithCooldown(closedAtSecs: number[], cooldownSec: number): number {
    let count = 0;
    let lastCountedSec: number | null = null;
    const sorted = [...closedAtSecs].sort((a, b) => a - b);
    for (const sec of sorted) {
        if (lastCountedSec == null || sec - lastCountedSec >= cooldownSec) {
            count += 1;
            lastCountedSec = sec;
        }
    }
    return count;
}

interface SquadTotalsRow {
    MemberID: number;
    total_squads: number;
    filled_squads: number;
}

interface RepRow {
    MemberID: number;
    TotalSquads: number;
    FilledSquads: number;
    AllTime: number;
}

interface MemberRow {
    MemberID: number;
    MemberName: string | null;
}

async function main(): Promise<void> {
    // 1) Per-member totals derived from squads (equivalent to summing per-day totals).
    const fromSquads = await SelectQuery<SquadTotalsRow>(
        `SELECT su.MemberID,
                COUNT(*) AS total_squads,
                COALESCE(SUM(s.Filled), 0) AS filled_squads
         FROM ${TABLES.SQUADS} s
         INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID
         WHERE s.ClosedAt IS NOT NULL
         GROUP BY su.MemberID`,
        []
    );

    // 2) memberreputation snapshot.
    const repRows = await SelectQuery<RepRow>(
        `SELECT MemberID, TotalSquads, FilledSquads, AllTime FROM ${TABLES.MEMBERREPUTATION}`,
        []
    );

    // 3) vrb_reputation and missing_reputation (added to AllTime in profiles/hover).
    const vrbRows = await SelectQuery<{ id: number; reputation: number }>(
        `SELECT id, reputation FROM ${TABLES.VRB_REPUTATION}`,
        []
    );
    const missingRows = await SelectQuery<{ id: number; reputation: number }>(
        `SELECT id, reputation FROM ${TABLES.MISSING_REPUTATION}`,
        []
    );

    // 4) Member names for more readable output.
    const memberRows = await SelectQuery<MemberRow>(
        `SELECT MemberID, MemberName FROM ${TABLES.MEMBERS}`,
        []
    );

    const fromSquadsByMember = new Map<number, { total_squads: number; filled_squads: number }>(
        fromSquads.map((r) => [r.MemberID, { total_squads: Number(r.total_squads), filled_squads: Number(r.filled_squads) }])
    );
    const repByMember = new Map<number, RepRow>(repRows.map((r) => [r.MemberID, r]));
    const nameByMember = new Map<number, string | null>(memberRows.map((m) => [m.MemberID, m.MemberName ?? null]));
    const vrbByMember = new Map<number, number>(vrbRows.map((r) => [r.id, Number(r.reputation)]));
    const missingByMember = new Map<number, number>(missingRows.map((r) => [r.id, Number(r.reputation)]));

    const memberIds = new Set<number>([
        ...fromSquadsByMember.keys(),
        ...repByMember.keys()
    ]);
    const TARGET_MEMBER_ID = 178760;

    // Mismatches we care about:
    // - TotalSquads (raw total squads)
    // - FilledSquads (raw filled squads)
    // - CumulativeWithCooldown (cooldown-applied filled + vrb + missing vs AllTime + vrb + missing)
    type MismatchType = 'TotalSquads' | 'FilledSquads' | 'CumulativeWithCooldown';
    type Mismatch = {
        memberId: number;
        memberName: string | null;
        type: MismatchType;
        fromSquads: number;
        fromRep: number;
    };

    /** Per-member values for logging cumulative vs all-time (including vrb + missing) and cooldown-applied counts. */
    const memberTotals = new Map<
        number,
        { dailyFilled: number; repAllTime: number; vrb: number; missing: number; countedWithCooldown: number }
    >();

    const mismatches: Mismatch[] = [];
    const missingRep: number[] = [];
    const noSquadsButRep: number[] = [];

    for (const memberId of memberIds) {
        if (memberId !== TARGET_MEMBER_ID) continue;
        const sq = fromSquadsByMember.get(memberId);
        const rep = repByMember.get(memberId);
        const name = nameByMember.get(memberId) ?? null;

        if (!rep) {
            if (sq && (sq.total_squads > 0 || sq.filled_squads > 0)) {
                missingRep.push(memberId);
            }
            continue;
        }

        const dailyTotal = sq?.total_squads ?? 0;
        const dailyFilled = sq?.filled_squads ?? 0;
        const repTotal = Number(rep.TotalSquads);
        const repFilled = Number(rep.FilledSquads);
        const repAllTime = Number(rep.AllTime);
        const vrb = vrbByMember.get(memberId) ?? 0;
        const missing = missingByMember.get(memberId) ?? 0;
        const effectiveAllTime = repAllTime + vrb + missing;

        // Fetch all filled squad close times for this member to recompute reputation with cooldown.
        const closedRows = await SelectQuery<{ closed_sec: number }>(
            `SELECT IF(s.ClosedAt >= 10000000000, FLOOR(s.ClosedAt/1000), s.ClosedAt) AS closed_sec
             FROM ${TABLES.SQUADS} s
             INNER JOIN ${TABLES.SQUADUSERS} su ON s.SquadID = su.SquadID AND su.MemberID = ?
             WHERE s.ClosedAt IS NOT NULL AND s.Filled = 1
             ORDER BY closed_sec`,
            [memberId]
        );
        const closedSecs = closedRows.map((r) => Number(r.closed_sec));
        const countedWithCooldown = countFilledWithCooldown(closedSecs, MEMBER_REPUTATION_COOLDOWN_SEC);

        memberTotals.set(memberId, { dailyFilled, repAllTime, vrb, missing, countedWithCooldown });

        if (dailyTotal !== repTotal) {
            mismatches.push({
                memberId,
                memberName: name,
                type: 'TotalSquads',
                fromSquads: dailyTotal,
                fromRep: repTotal
            });
        }
        if (dailyFilled !== repFilled) {
            mismatches.push({
                memberId,
                memberName: name,
                type: 'FilledSquads',
                fromSquads: dailyFilled,
                fromRep: repFilled
            });
        }

        // Cumulative WITH cooldown: cooldown-applied filled + vrb + missing
        const cumulativeWithCooldown = countedWithCooldown + vrb + missing;
        const allTimeWithExtras = effectiveAllTime; // AllTime + vrb + missing
        if (cumulativeWithCooldown !== allTimeWithExtras) {
            mismatches.push({
                memberId,
                memberName: name,
                type: 'CumulativeWithCooldown',
                fromSquads: cumulativeWithCooldown,
                fromRep: allTimeWithExtras
            });
        }

        if (!sq && (repTotal > 0 || repFilled > 0 || effectiveAllTime > 0)) {
            noSquadsButRep.push(memberId);
        }
    }

    // Summary output.
    console.log('Member reputation sum check (cumulative with cooldown vs memberreputation)\n');
    console.log(`Members with reputation row: ${repByMember.size}`);
    console.log(`Members with closed squad activity: ${fromSquadsByMember.size}`);

    // Log daily, cooldown-applied, and all-time (including vrb + missing) for every member with a rep row.
    console.log(
        '\nReputation values (for target member): daily, cooldown-applied, and all-time including vrb_reputation + missing_reputation:'
    );
    const sortedMemberIds = [...memberTotals.keys()].sort((a, b) => a - b);
    for (const memberId of sortedMemberIds) {
        const t = memberTotals.get(memberId)!;
        const cooldownCount = t.countedWithCooldown;
        const cumulativeIncl = cooldownCount + t.vrb + t.missing;
        const allTimeIncl = t.repAllTime + t.vrb + t.missing;
        const name = nameByMember.get(memberId) ?? null;
        const label = name ? `${memberId} (${name})` : String(memberId);
        console.log(
            `  ${label}: dailyFilled=${t.dailyFilled}, cooldownCount=${cooldownCount}, repAllTime=${t.repAllTime}, cumulative=${cumulativeIncl}, all-time=${allTimeIncl}`
        );
    }

    if (missingRep.length > 0) {
        console.log(
            `\nMembers with squad activity but no memberreputation row: ${missingRep.length} — ` +
                `${missingRep.slice(0, 20).join(', ')}${missingRep.length > 20 ? '...' : ''}`
        );
    }
    if (noSquadsButRep.length > 0) {
        console.log(
            `\nMembers with reputation row but no closed squads in DB: ${noSquadsButRep.length} — ` +
                `${noSquadsButRep.slice(0, 20).join(', ')}${noSquadsButRep.length > 20 ? '...' : ''}`
        );
    }

    if (mismatches.length === 0) {
        console.log('\nNo mismatches: cumulative daily totals match stored totals for all members.');
        return;
    }

    console.log(`\nMismatches (${mismatches.length}):`);
    const byMember = new Map<number, Mismatch[]>();
    for (const m of mismatches) {
        if (!byMember.has(m.memberId)) byMember.set(m.memberId, []);
        byMember.get(m.memberId)!.push(m);
    }

    for (const [memberId, list] of byMember) {
        const name = list[0].memberName;
        const label = name ? `${memberId} (${name})` : String(memberId);
        const totals = memberTotals.get(memberId);
        const cumulativeIncl = totals
            ? totals.countedWithCooldown + totals.vrb + totals.missing
            : null;
        const allTimeIncl = totals
            ? totals.repAllTime + totals.vrb + totals.missing
            : null;

        const parts = list.map(
            (m) => `${m.type}: squads=${m.fromSquads} rep=${m.fromRep}`
        );
        console.log(`  Member ${label}: ${parts.join('; ')}`);
        if (cumulativeIncl !== null && allTimeIncl !== null) {
            console.log(
                `    Cumulative (with cooldown + vrb + missing): ${cumulativeIncl}`
            );
            console.log(
                `    All-time (from memberreputation + vrb + missing): ${allTimeIncl}`
            );
        }
    }
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});

