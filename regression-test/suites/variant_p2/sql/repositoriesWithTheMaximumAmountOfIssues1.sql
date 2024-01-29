SELECT cast(repo["name"] as string), count() AS c, count(distinct cast(actor["login"] as string)) AS u FROM github_events WHERE type = 'IssuesEvent' AND cast(payload["action"] as string) = 'opened' GROUP BY cast(repo["name"] as string) ORDER BY c DESC, cast(repo["name"] as string) LIMIT 50
