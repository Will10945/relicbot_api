-- vrb_reputation: legacy reputation from before tracking began (before 2021-09-21).
-- No timestamps; these values are included only in all-time / total-squads style totals.
-- id references members.MemberID; reputation is 0–3000.

CREATE TABLE IF NOT EXISTS `vrb_reputation` (
  `id` int NOT NULL,
  `reputation` int unsigned NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `vrb_reputation_fk_member` FOREIGN KEY (`id`) REFERENCES `members` (`MemberID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
