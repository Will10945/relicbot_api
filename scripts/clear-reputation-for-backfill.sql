-- Clear reputation and friends tables so backfill-reputation will process ALL closed squads from scratch.
-- Does NOT touch squads or any squad data. Run this, then: npm run backfill-reputation

SET FOREIGN_KEY_CHECKS = 0;

-- Backfill state: clear so every closed squad is considered unprocessed
TRUNCATE TABLE reputation_backfill_processed;

-- Host/reputation: hostreputation references hosts, so truncate before hosts
TRUNCATE TABLE hostreputation;
TRUNCATE TABLE hosts;

-- Reputation stats (member/relic from squads)
TRUNCATE TABLE memberreputation;
TRUNCATE TABLE relicreputation;

-- Friends (SquadsTogether, FilledSquads, etc. from squad activity)
TRUNCATE TABLE relicfriends;
TRUNCATE TABLE relicpairfriends;
TRUNCATE TABLE memberfriends;
TRUNCATE TABLE offcyclefriends;

SET FOREIGN_KEY_CHECKS = 1;
