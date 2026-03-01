-- Add Offcycle to relicfriends so we track member-relic usage as on-cycle (0) or off-cycle (1).
-- Primary key becomes (MemberID, RelicID, Offcycle). Existing rows get Offcycle = 0.
-- Drop FKs first because MySQL cannot drop the PRIMARY key while it is used by the table's own FKs.

ALTER TABLE `relicfriends` ADD COLUMN `Offcycle` tinyint(1) NOT NULL DEFAULT 0 AFTER `RelicID`;
ALTER TABLE `relicfriends` DROP FOREIGN KEY `rf_fk_member`, DROP FOREIGN KEY `rf_fk_relic`;
ALTER TABLE `relicfriends` DROP PRIMARY KEY;
ALTER TABLE `relicfriends` ADD PRIMARY KEY (`MemberID`, `RelicID`, `Offcycle`);
ALTER TABLE `relicfriends`
  ADD CONSTRAINT `rf_fk_member` FOREIGN KEY (`MemberID`) REFERENCES `members` (`MemberID`) ON DELETE CASCADE,
  ADD CONSTRAINT `rf_fk_relic` FOREIGN KEY (`RelicID`) REFERENCES `relics` (`ID`);
