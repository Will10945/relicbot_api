-- Covering index for "find SquadIDs by RelicID" (faster squad-by-relic filtering).
-- Run once; omit if the index already exists.
CREATE INDEX idx_squadrelics_relic_squad ON squadrelics (RelicID, SquadID);
