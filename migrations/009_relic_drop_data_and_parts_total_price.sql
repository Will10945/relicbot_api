-- relic_drop_data: per-relic, per-part pricing and rarity from relics.run (sync).
-- primesets: add PartsTotalPrice (sum of part prices * required); ducats stays single value.

-- Add PartsTotalPrice to primesets (nullable for existing rows; sync will fill).
ALTER TABLE `primesets`
  ADD COLUMN `PartsTotalPrice` int DEFAULT NULL AFTER `Ducats`;

-- Table for relic drop data (RelicID, PartName, Rarity, Ducats, Price, Chances JSON).
CREATE TABLE IF NOT EXISTS `relic_drop_data` (
  `RelicID` int NOT NULL,
  `PartName` varchar(100) NOT NULL,
  `Rarity` varchar(50) NOT NULL,
  `Ducats` int NOT NULL,
  `Price` int NOT NULL,
  `Chances` json DEFAULT NULL,
  PRIMARY KEY (`RelicID`, `PartName`),
  CONSTRAINT `relic_drop_data_fk_relic` FOREIGN KEY (`RelicID`) REFERENCES `relics` (`ID`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
