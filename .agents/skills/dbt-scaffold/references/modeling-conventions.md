# Modeling conventions

Use source → staging → intermediate → marts. Staging only renames, casts, and normalizes one source.
Intermediate models hold reusable joins, sequence, and deduplication. Published marts declare grain,
owner, contract, tests, descriptions, SLO, and consumers. Never invent source columns.
