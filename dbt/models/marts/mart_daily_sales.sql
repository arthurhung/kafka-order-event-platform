{{ config(contract={'enforced': true}) }}

with attributed as (
    select
        events.event_date,
        case
            when events.event_type = 'order_cancelled' then orders.currency
            else events.currency
        end as currency,
        orders.channel,
        events.order_id,
        events.event_type,
        events.amount
    from {{ ref('int_order_event_sequence') }} as events
    left join {{ ref('int_order_latest_state') }} as orders using (order_id)
),

aggregated as (
    select
        event_date,
        currency,
        channel,
        count(distinct order_id) filter (where event_type = 'order_created')::bigint
            as created_order_count,
        coalesce(sum(amount) filter (where event_type = 'order_created'), 0)::numeric(18, 2)
            as created_order_amount,
        count(distinct order_id) filter (where event_type = 'order_paid')::bigint
            as paid_order_count,
        coalesce(sum(amount) filter (where event_type = 'order_paid'), 0)::numeric(18, 2)
            as paid_amount,
        count(distinct order_id) filter (where event_type = 'order_cancelled')::bigint
            as cancelled_order_count,
        count(distinct order_id) filter (where event_type = 'payment_failed')::bigint
            as payment_failed_order_count,
        coalesce(sum(amount) filter (where event_type = 'payment_failed'), 0)::numeric(18, 2)
            as payment_failed_amount,
        count(*) filter (where event_type in ('order_paid', 'payment_failed'))::bigint
            as payment_attempt_count,
        count(*) filter (where event_type = 'order_paid')::bigint as payment_success_count
    from attributed
    group by event_date, currency, channel
)

select
    event_date,
    currency,
    channel,
    created_order_count,
    created_order_amount,
    paid_order_count,
    paid_amount,
    cancelled_order_count,
    payment_failed_order_count,
    payment_failed_amount,
    payment_attempt_count,
    {{ safe_divide('payment_success_count', 'payment_attempt_count') }} as payment_success_rate
from aggregated
