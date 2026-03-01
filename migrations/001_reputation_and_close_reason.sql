-- Reputation: TotalSquads + close reason. Run once on existing DB.
-- Running ALTER twice will error "Duplicate column"; ignore or skip.

-- Squads: close reason (Filled is set at close time in app)
ALTER TABLE `squads` ADD COLUMN `CloseReason` varchar(64) DEFAULT NULL AFTER `ClosedAt`;
ALTER TABLE `squadstest` ADD COLUMN `CloseReason` varchar(64) DEFAULT NULL AFTER `ClosedAt`;

-- Member reputation: all-time total squads (filled or not)
ALTER TABLE `memberreputation` ADD COLUMN `TotalSquads` bigint unsigned NOT NULL DEFAULT 0 AFTER `AllTime`;

-- Host reputation: all-time total squads
ALTER TABLE `hostreputation` ADD COLUMN `TotalSquads` bigint unsigned NOT NULL DEFAULT 0 AFTER `AllTime`;

-- Relic reputation: all-time total squads
ALTER TABLE `relicreputation` ADD COLUMN `TotalSquads` bigint unsigned NOT NULL DEFAULT 0 AFTER `AllTime`;

-- Backfill script tracking (idempotent runs)
CREATE TABLE IF NOT EXISTS `reputation_backfill_processed` (
  `SquadID` char(36) NOT NULL,
  `ProcessedAt` bigint NOT NULL,
  PRIMARY KEY (`SquadID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
