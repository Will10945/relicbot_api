/**
 * One-time (or resumable) backfill of reputation and friends tables from closed squads.
 * Run after applying migrations/001_reputation_and_close_reason.sql.
 *
 * Usage: npx ts-node src/scripts/backfill-reputation.ts
 * Or: npm run backfill-reputation
 *
 * Skips squads already in reputation_backfill_processed. Safe to run multiple times.
 * Uses one transaction per batch (aggregate + bulk upsert) for maximum speed.
 */

import {
    getClosedSquadIdsNotYetProcessedForReputation,
    getSquadsByIds,
    processReputationBackfillBatch
} from '../database/database';

const BATCH_SIZE = 2000;
const DEADLOCK_RETRY_ATTEMPTS = 3;
const DEADLOCK_RETRY_DELAY_MS = 1000;

/** Optional cap: only process squads with ClosedAt <= this (seconds). Use to match manual counts up to a timestamp. */
const _maxClosed = process.env.MAX_CLOSED_AT != null && process.env.MAX_CLOSED_AT !== '' ? parseInt(process.env.MAX_CLOSED_AT, 10) : NaN;
const MAX_CLOSED_AT_SEC = Number.isFinite(_maxClosed) ? _maxClosed : undefined;

function isDeadlock(err: unknown): boolean {
    const code = err && typeof err === 'object' && 'errno' in (err as object) ? (err as { errno: number }).errno : 0;
    return code === 1213;
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
    const capMsg = MAX_CLOSED_AT_SEC != null && Number.isFinite(MAX_CLOSED_AT_SEC) ? ` (max ClosedAt ${MAX_CLOSED_AT_SEC} s)` : '';
    console.log(`Starting reputation backfill (batch size ${BATCH_SIZE}, one transaction per batch${capMsg}).`);
    let total = 0;
    let batch = 0;
    let failed = 0;
    while (true) {
        const squadIds = await getClosedSquadIdsNotYetProcessedForReputation(BATCH_SIZE, MAX_CLOSED_AT_SEC);
        if (squadIds.length === 0) {
            console.log('No more squads to process.');
            break;
        }
        batch++;
        const batchStart = Date.now();
        const { squads, squadUsers, squadRelics, squadRefinements } = await getSquadsByIds(squadIds);
        let ok = false;
        let lastErr: unknown;
        for (let attempt = 1; attempt <= DEADLOCK_RETRY_ATTEMPTS; attempt++) {
            try {
                await processReputationBackfillBatch(squads, squadUsers, squadRelics, squadRefinements);
                ok = true;
                break;
            } catch (err) {
                lastErr = err;
                if (attempt < DEADLOCK_RETRY_ATTEMPTS && isDeadlock(err)) {
                    console.warn(`Batch ${batch} deadlock (attempt ${attempt}/${DEADLOCK_RETRY_ATTEMPTS}), retrying...`);
                    await sleep(DEADLOCK_RETRY_DELAY_MS);
                } else {
                    break;
                }
            }
        }
        if (ok) {
            const processed = squads.filter((s) => s.ClosedAt != null).length;
            total += processed;
        } else {
            failed += squadIds.length;
            console.error(`Batch ${batch} failed:`, lastErr);
            const code = lastErr && typeof lastErr === 'object' && 'errno' in (lastErr as object) ? (lastErr as { errno: number }).errno : 0;
            if (code === 1205) {
                console.warn('Lock wait timeout: run backfill again later (e.g. when API is idle).');
            }
        }
        const elapsed = ((Date.now() - batchStart) / 1000).toFixed(1);
        console.log(`Batch ${batch}: ${squadIds.length} squads in ${elapsed}s (total ok: ${total}, failed batches: ${failed}).`);
    }
    console.log(`Backfill complete. Processed ${total} squads.`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
