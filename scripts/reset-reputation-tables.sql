-- Reset all reputation and friends tables for a clean backfill.
-- Run this, then run: npm run backfill-reputation
-- Order respects foreign keys (hostreputation -> hosts).

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE reputation_backfill_processed;
TRUNCATE TABLE memberreputation;
TRUNCATE TABLE hostreputation;
TRUNCATE TABLE hosts;
TRUNCATE TABLE relicreputation;
TRUNCATE TABLE memberfriends;
TRUNCATE TABLE relicfriends;
TRUNCATE TABLE relicpairfriends;

SET FOREIGN_KEY_CHECKS = 1;
