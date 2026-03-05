-- Fix Rarity column length (relics.run may send values longer than varchar(20)).
ALTER TABLE `relic_drop_data`
  MODIFY COLUMN `Rarity` varchar(50) NOT NULL;
