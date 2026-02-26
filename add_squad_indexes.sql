-- Add indexes for squad API query patterns (database.ts / squads.ts).
-- Run on an existing DB to apply. Safe to run if indexes already exist (ignore errors or check first).

-- Active squads: WHERE Active = 1 AND CreatedAt >= ? ORDER BY CreatedAt DESC (getActiveSquads, getActiveSquadIdsByMemberId join)
ALTER TABLE `squads` ADD KEY `idx_squads_active_createdat` (`Active`, `CreatedAt`);

-- Leave context: WHERE MemberID = ? AND SquadID IN (...) (getLeaveContextBatch)
ALTER TABLE `squadusers` ADD KEY `idx_squadusers_member_squad` (`MemberID`, `SquadID`);
