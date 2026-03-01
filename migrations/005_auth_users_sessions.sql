-- Auth: users (login accounts) and sessions (persistent session store).

CREATE TABLE IF NOT EXISTS `users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL,
  `password_hash` varchar(255) NOT NULL,
  `created_at` int NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `users_username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `sessions` (
  `id` varchar(64) NOT NULL,
  `user_id` int unsigned NOT NULL,
  `created_at` int NOT NULL,
  `expires_at` int NOT NULL,
  PRIMARY KEY (`id`),
  KEY `sessions_user_id` (`user_id`),
  KEY `sessions_expires_at` (`expires_at`),
  CONSTRAINT `sessions_fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
