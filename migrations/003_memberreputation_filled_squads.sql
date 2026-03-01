-- Add FilledSquads to memberreputation (total filled squads, no cooldown).
-- AllTime = reputation with 20-min cooldown; FilledSquads = every filled squad.
-- Column order: ... AllTime, FilledSquads, TotalSquads, LastUpdate.
-- If FilledSquads already exists (e.g. added after TotalSquads): ALTER TABLE memberreputation DROP COLUMN FilledSquads; then run the line below.

ALTER TABLE `memberreputation` ADD COLUMN `FilledSquads` bigint unsigned NOT NULL DEFAULT 0 AFTER `AllTime`;
