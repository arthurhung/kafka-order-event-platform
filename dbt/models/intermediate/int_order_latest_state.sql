with events as (
    select * from {{ ref('int_order_event_sequence') }}
),

aggregated as (
    select
        order_id,
        min(event_time) as earliest_observed_event_time,
        count(*)::bigint as order_event_count,
        (array_agg(user_id order by event_sequence_number))[1] as user_id,
        (array_agg(product_id order by event_sequence_number)
            filter (where event_type = 'order_created'))[1] as product_id,
        (array_agg(quantity order by event_sequence_number)
            filter (where event_type = 'order_created'))[1] as quantity,
        (array_agg(channel order by event_sequence_number)
            filter (where event_type = 'order_created'))[1] as channel,
        (array_agg(currency order by event_sequence_number)
            filter (where event_type = 'order_created'))[1] as currency,
        (array_agg(amount order by event_sequence_number)
            filter (where event_type = 'order_created'))[1] as original_order_amount,
        (array_agg(event_time order by event_sequence_number)
            filter (where event_type = 'order_created'))[1] as order_created_at,
        (array_agg(event_time order by event_sequence_number)
            filter (where event_type = 'order_paid'))[1] as first_paid_at,
        (array_agg(event_time order by event_sequence_number desc)
            filter (where event_type = 'order_paid'))[1] as latest_paid_at,
        (array_agg(amount order by event_sequence_number desc)
            filter (where event_type = 'order_paid'))[1] as latest_paid_amount,
        (array_agg(currency order by event_sequence_number desc)
            filter (where event_type = 'order_paid'))[1] as latest_paid_currency,
        (array_agg(event_time order by event_sequence_number desc)
            filter (where event_type = 'payment_failed'))[1] as latest_payment_failed_at,
        (array_agg(event_time order by event_sequence_number)
            filter (where event_type = 'order_cancelled'))[1] as cancelled_at,
        count(*) filter (where event_type in ('order_paid', 'payment_failed'))::bigint
            as payment_attempt_count,
        count(*) filter (where event_type = 'payment_failed')::bigint
            as payment_failure_count,
        bool_or(event_type = 'order_paid') as is_paid,
        bool_or(event_type = 'order_cancelled') as is_cancelled
    from events
    group by order_id
),

latest as (
    select distinct on (order_id)
        order_id,
        event_time as latest_event_at,
        event_type as latest_event_type,
        case event_type
            when 'order_created' then 'created'
            when 'order_paid' then 'paid'
            when 'order_cancelled' then 'cancelled'
            when 'payment_failed' then 'payment_failed'
        end::varchar(20) as latest_order_state
    from events
    order by order_id, event_sequence_number desc, event_id desc
)

select aggregated.*, latest.latest_event_at, latest.latest_event_type, latest.latest_order_state
from aggregated
join latest using (order_id)
