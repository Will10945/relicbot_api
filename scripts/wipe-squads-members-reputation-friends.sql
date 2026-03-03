-- Wipe squads and everything built from squads (reputation, friends, host data).
-- Does NOT touch members or member-only tables.
-- Usage: mysql -u ... -p ... < scripts/wipe-squads-members-reputation-friends.sql
-- Or run via: npm run wipe-squads-members

SET FOREIGN_KEY_CHECKS = 0;

-- Squad child tables first (they reference squads). squadhost also references hosts.
TRUNCATE TABLE squadposts;
TRUNCATE TABLE squadrefinement;
TRUNCATE TABLE squadrelics;
TRUNCATE TABLE squadusers;
TRUNCATE TABLE squadhost;
TRUNCATE TABLE squads;

-- Host/reputation: hostreputation references hosts, so truncate before hosts.
TRUNCATE TABLE reputation_backfill_processed;
TRUNCATE TABLE hostreputation;
TRUNCATE TABLE hosts;

-- Friends (SquadsTogether, FilledSquads, etc. from squad activity)
TRUNCATE TABLE relicfriends;
TRUNCATE TABLE relicpairfriends;
TRUNCATE TABLE memberfriends;
TRUNCATE TABLE offcyclefriends;

-- Reputation stats (member/host/relic from squads)
TRUNCATE TABLE memberreputation;
TRUNCATE TABLE relicreputation;

SET FOREIGN_KEY_CHECKS = 1;
