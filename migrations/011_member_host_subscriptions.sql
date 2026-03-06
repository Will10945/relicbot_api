-- Host subscriptions: member favorites for hosts (by HostID).
-- Hosts are identified by SignatureHash; use get-or-create when adding so new hosts can be subscribed before they exist.
CREATE TABLE IF NOT EXISTS `memberhostsubscriptions` (
  `MemberID` int NOT NULL,
  `HostID` bigint NOT NULL,
  PRIMARY KEY (`MemberID`, `HostID`),
  KEY `HostID` (`HostID`),
  CONSTRAINT `memberhostsubscriptions_fk_member` FOREIGN KEY (`MemberID`) REFERENCES `members` (`MemberID`) ON DELETE CASCADE,
  CONSTRAINT `memberhostsubscriptions_fk_host` FOREIGN KEY (`HostID`) REFERENCES `hosts` (`HostID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
