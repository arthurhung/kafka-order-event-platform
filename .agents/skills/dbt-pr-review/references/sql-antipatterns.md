# SQL antipatterns

Block direct source use outside staging, published `select *`, cross joins without a reviewed grain,
multi-currency totals, and unsafe incremental watermarks. Warn on unguarded division, unweighted
average-of-averages, and possible full scans.
