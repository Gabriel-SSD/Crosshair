INSERT INTO `gcp_echo_silver.silver_tw_leaderboard` (
  event_date,
  member_id,
  guild_id,
  total_banners,
  attack_banners,
  defense_banners,
  rogue_actions
)
SELECT
  DATE(TIMESTAMP_SECONDS(CAST(CAST(REGEXP_EXTRACT(b.territoryMapId, r'O(\d+)$') AS INT64) / 1000 AS INT64))) AS event_date,
  total.memberId AS member_id,
  b.homeGuildProfile.id AS guild_id,
  total.banners AS total_banners,
  atk.banners AS attack_banners,
  def.banners AS defense_banners,
  rogue.rogueActions AS rogue_actions
FROM
  `gcp_echo_bronze.bronze_tw_leaderboard` b
  LEFT JOIN UNNEST(b.data.totalBanners) AS total
  LEFT JOIN UNNEST(b.data.attackBanners) AS atk ON atk.memberId = total.memberId
  LEFT JOIN UNNEST(b.data.defenseBanners) AS def ON def.memberId = total.memberId
  LEFT JOIN UNNEST(b.data.rogueActions) AS rogue ON rogue.memberId = total.memberId
WHERE
  DATE(TIMESTAMP_SECONDS(CAST(CAST(REGEXP_EXTRACT(b.territoryMapId, r'O(\d+)$') AS INT64) / 1000 AS INT64))) > DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY)
  AND NOT EXISTS (
    SELECT 1
    FROM `gcp_echo_silver.silver_tw_leaderboard` s
    WHERE
      s.event_date = DATE(TIMESTAMP_SECONDS(CAST(CAST(REGEXP_EXTRACT(b.territoryMapId, r'O(\d+)$') AS INT64) / 1000 AS INT64)))
      AND s.guild_id = b.homeGuildProfile.id
  )
