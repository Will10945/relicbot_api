-- Link a user (auth) to a member (game/Discord). Optional; one member per user.

ALTER TABLE `users`
  ADD COLUMN `member_id` int NULL DEFAULT NULL AFTER `created_at`,
  ADD KEY `users_member_id` (`member_id`),
  ADD CONSTRAINT `users_fk_member` FOREIGN KEY (`member_id`) REFERENCES `members` (`MemberID`) ON DELETE SET NULL;
