-- SELECT
--     cast(v["repo"]["name"] as string),
--     day,
--     stars
-- FROM
-- (
--     SELECT
--         row_number() OVER (PARTITION BY cast(v["repo"]["name"] as string) ORDER BY stars DESC) AS rank,
--         cast(v["repo"]["name"] as string),
--         day,
--         stars
--     FROM
--     (
--         SELECT
--             cast(v["repo"]["name"] as string),
--             to_date(cast(v["created_at"] as datetime)) AS day,
--             count() AS stars
--         FROM github_events
--         WHERE cast(v["type"] as string) = 'WatchEvent'
--         GROUP BY cast(v["repo"]["name"] as string), day
--     ) t1
-- ) t2
-- WHERE rank = 1
-- ORDER BY stars DESC
-- LIMIT 50
