import pytest

from data_platform.bigquery_sql_policy import analyze_sql, mask_comments_and_literals


def _analyze(sql: str, **kwargs):
    return analyze_sql(
        "mart_daily_sales",
        sql,
        partition_field="event_date",
        require_partition_filter=True,
        monetary=kwargs.get("monetary", False),
        incremental=kwargs.get("incremental", False),
    )


def test_valid_bounded_sql_passes():
    findings = _analyze("select event_date from mart where event_date >= x and event_date < y")
    assert not [item for item in findings if item.severity == "error"]


def test_postgres_syntax_warns_but_comment_and_literal_do_not():
    findings = _analyze("select value::numeric from mart where event_date >= x and event_date < y")
    assert any(item.rule == "postgres-cast" and item.severity == "warning" for item in findings)
    clean = _analyze(
        "-- select * ::numeric\n"
        "select event_date from mart where event_date >= x and event_date < y "
        "and note = 'DISTINCT ON bool_or'"
    )
    assert not [
        item for item in clean if item.rule.startswith("postgres-") or item.rule == "select-star"
    ]


def test_backticks_and_escaped_quotes_are_safely_masked():
    masked = mask_comments_and_literals("select `select *`, 'it''s ::bad' from mart")
    assert "::bad" not in masked
    assert "select *" not in masked


@pytest.mark.parametrize("sql", ["select 'unterminated", "select /* unterminated"])
def test_unbalanced_sql_is_error(sql: str):
    findings = _analyze(sql)
    assert findings[0].rule == "sql-unknown"
    assert findings[0].severity == "error"


def test_full_scan_select_star_and_cross_join_block():
    findings = _analyze("select * from a cross join b")
    rules = {item.rule for item in findings if item.severity == "error"}
    assert {"select-star", "cross-join", "partition-predicate"} <= rules


def test_currency_and_incremental_lookback_block():
    findings = _analyze("select sum(paid_amount) from mart", monetary=True, incremental=True)
    rules = {item.rule for item in findings if item.severity == "error"}
    assert {"currency-grouping", "partition-predicate", "incremental-lookback"} <= rules


def test_pruning_hostile_transformation_warns():
    findings = _analyze(
        "select event_date from mart where date(event_date) >= x and event_date < y"
    )
    assert any(item.rule == "partition-transformation" for item in findings)


def test_group_by_commas_are_not_comma_joins():
    findings = _analyze(
        "select event_date, currency from mart "
        "where event_date >= x and event_date < y group by event_date, currency"
    )
    assert not [item for item in findings if item.rule == "comma-join"]


def test_actual_comma_join_warns():
    findings = _analyze(
        "select event_date from mart, other where event_date >= x and event_date < y"
    )
    assert any(item.rule == "comma-join" for item in findings)
