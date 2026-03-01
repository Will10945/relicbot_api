-- Add FilledSquads to friends tables (total = SquadsTogether; filled = FilledSquads).
-- Run once. Safe to re-run only if column is missing (MySQL has no IF NOT EXISTS for columns).

ALTER TABLE `memberfriends` ADD COLUMN `FilledSquads` int unsigned NOT NULL DEFAULT 0 AFTER `SquadsTogether`;
ALTER TABLE `relicfriends` ADD COLUMN `FilledSquads` int unsigned NOT NULL DEFAULT 0 AFTER `SquadsTogether`;
ALTER TABLE `relicpairfriends` ADD COLUMN `FilledSquads` int unsigned NOT NULL DEFAULT 0 AFTER `SquadsTogether`;
