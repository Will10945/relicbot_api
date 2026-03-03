-- Indexes to speed up squads list and filtered queries. Run once; omit any that already exist.

-- Active squads (24h window): WHERE Active = 1 AND CreatedAt >= ?
CREATE INDEX squads_active_created_at ON squads (Active, CreatedAt);

-- Simple filters used by GET /api/squads
CREATE INDEX squads_style ON squads (Style);
CREATE INDEX squads_era ON squads (Era);
CREATE INDEX squads_host ON squads (Host);
CREATE INDEX squads_filled ON squads (Filled);
CREATE INDEX squads_originating_server ON squads (OriginatingServer);

-- Ordering for paginated list (CreatedAt DESC)
CREATE INDEX squads_created_at ON squads (CreatedAt);

-- Related tables: lookups by SquadID
CREATE INDEX squadusers_squad_id ON squadusers (SquadID);
CREATE INDEX squadrelics_squad_id ON squadrelics (SquadID);
CREATE INDEX squadrefinement_squad_id ON squadrefinement (SquadID);
