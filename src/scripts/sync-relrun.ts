/**
 * Sync primesets, primeparts, relic_drop_data and relic vaulted status from relics.run get_all_data.
 * Run once per day (e.g. cron at 02:00 UTC). Apply migration 009 before first run.
 *
 * Usage: npx ts-node src/scripts/sync-relrun.ts
 * Or: npm run sync-relrun
 *
 * Env: MYSQL_* (same as API). Optional RELRUN_SYNC_CRON for scheduling hint (not used by this script).
 */

import type { RelrunSyncPayload } from '../database/database';
import { getAllRelics, runRelrunSync } from '../database/database';

const RELRUN_URL = 'https://relics.run/get_all_data';

interface RelrunSetPart {
    ducats: number;
    price: number;
    required: number;
}

interface RelrunSet {
    name: string;
    price: number;
    total_ducats: number;
    type: string;
    parts: Record<string, RelrunSetPart>;
}

interface RelrunDrop {
    price: number;
    ducats: number;
    rarity: string;
    chances?: Record<string, number>;
}

interface RelrunRelic {
    name: string;
    vaulted: boolean;
    drops: Record<string, RelrunDrop>;
}

interface RelrunData {
    sets: Record<string, RelrunSet>;
    relics: Record<string, RelrunRelic>;
    vaulted_relics: string[];
}

function normalizeSetName(key: string): string {
    return key.replace(/\s+Set$/, '');
}

function isRelrunData(data: unknown): data is RelrunData {
    if (data == null || typeof data !== 'object') return false;
    const d = data as Record<string, unknown>;
    if (typeof d.sets !== 'object' || d.sets == null) return false;
    if (typeof d.relics !== 'object' || d.relics == null) return false;
    if (!Array.isArray(d.vaulted_relics)) return false;
    return true;
}

async function main(): Promise<void> {
    console.log('Fetching', RELRUN_URL, '...');
    const res = await fetch(RELRUN_URL);
    if (!res.ok) throw new Error(`relics.run fetch failed: ${res.status} ${res.statusText}`);
    const data: unknown = await res.json();
    if (!isRelrunData(data)) throw new Error('Invalid get_all_data shape: expected sets, relics, vaulted_relics');

    const vaultedSet = new Set(data.vaulted_relics);

    // partName -> set of relic keys that drop this part
    const partToRelicKeys = new Map<string, Set<string>>();
    for (const [relicKey, relic] of Object.entries(data.relics)) {
        if (!relic.drops) continue;
        for (const partName of Object.keys(relic.drops)) {
            let set = partToRelicKeys.get(partName);
            if (!set) {
                set = new Set();
                partToRelicKeys.set(partName, set);
            }
            set.add(relicKey);
        }
    }

    const relics = await getAllRelics();
    const keyToId = new Map<string, number>();
    for (const r of relics) {
        const key = `${r.Era ?? ''} ${r.Name ?? ''}`.trim();
        keyToId.set(key, r.ID);
    }

    const primeSets: RelrunSyncPayload['primeSets'] = [];
    const primeParts: RelrunSyncPayload['primeParts'] = [];
    const relicDrops: RelrunSyncPayload['relicDrops'] = [];
    const relicVaultedById: RelrunSyncPayload['relicVaultedById'] = [];

    for (const [setKey, set] of Object.entries(data.sets)) {
        const PrimeSet = normalizeSetName(setKey);
        let partsTotalPrice = 0;
        for (const part of Object.values(set.parts)) {
            partsTotalPrice += (part.price ?? 0) * (part.required ?? 1);
        }
        const partNames = Object.keys(set.parts);
        const relicKeysDroppingSet = new Set<string>();
        for (const pn of partNames) {
            const keys = partToRelicKeys.get(pn);
            if (keys) keys.forEach((k) => relicKeysDroppingSet.add(k));
        }
        const allVaulted =
            relicKeysDroppingSet.size > 0 &&
            [...relicKeysDroppingSet].every((k) => vaultedSet.has(k));
        const Vaulted = allVaulted ? 1 : 0;

        primeSets.push({
            PrimeSet,
            Price: set.price ?? 0,
            Ducats: set.total_ducats ?? 0,
            PartsTotalPrice: partsTotalPrice,
            Category: set.type ?? '',
            Vaulted
        });

        for (const [partName, part] of Object.entries(set.parts)) {
            primeParts.push({
                PrimeSet,
                Part: partName,
                Price: part.price ?? 0,
                Ducats: part.ducats ?? 0,
                Required: part.required ?? 1
            });
        }
    }

    for (const [relicKey, relic] of Object.entries(data.relics)) {
        const RelicID = keyToId.get(relicKey);
        if (RelicID != null) {
            relicVaultedById.push({
                RelicID,
                Vaulted: relic.vaulted ? 1 : 0
            });
        }
        if (RelicID == null) continue;
        if (!relic.drops) continue;
        for (const [partName, drop] of Object.entries(relic.drops)) {
            relicDrops.push({
                RelicID,
                PartName: partName,
                Rarity: drop.rarity ?? 'Common',
                Ducats: drop.ducats ?? 0,
                Price: drop.price ?? 0,
                Chances: drop.chances != null ? JSON.stringify(drop.chances) : null
            });
        }
    }

    console.log(
        `Upserting ${primeSets.length} primesets, ${primeParts.length} primeparts, ${relicDrops.length} relic drops, ${relicVaultedById.length} relic vaulted updates.`
    );
    await runRelrunSync({
        primeSets,
        primeParts,
        relicDrops,
        relicVaultedById
    });
    console.log('Sync complete.');
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
