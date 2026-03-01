-- Counts for member ID 1, only squads the backfill would process (Active=0, ClosedAt set)
-- and closed on or before the given timestamp. ClosedAt is in seconds (Unix timestamp).

-- 1) Number of FILLED squads (Filled = 1) that member 1 is in
SELECT COUNT(DISTINCT s.SquadID) AS filled_squads
FROM squads s
INNER JOIN squadusers su ON su.SquadID = s.SquadID
WHERE su.MemberID = 1
  AND s.Active = 0
  AND s.ClosedAt IS NOT NULL
  AND s.ClosedAt <= 1670681000
  AND s.Filled = 1;

-- 2) Number of ALL squads (any) that member 1 is in
SELECT COUNT(DISTINCT s.SquadID) AS total_squads
FROM squads s
INNER JOIN squadusers su ON su.SquadID = s.SquadID
WHERE su.MemberID = 1
  AND s.Active = 0
  AND s.ClosedAt IS NOT NULL
  AND s.ClosedAt <= 1670681000;
