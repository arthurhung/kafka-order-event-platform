{{ config(contract={'enforced': true}) }}

select
    order_id,
    user_id,
    product_id,
    quantity,
    currency,
    channel,
    order_created_at,
    first_paid_at,
    latest_paid_at,
    latest_payment_failed_at,
    cancelled_at,
    latest_event_at,
    latest_event_type,
    latest_order_state,
    original_order_amount,
    latest_paid_amount,
    latest_paid_currency,
    payment_attempt_count,
    payment_failure_count,
    order_event_count,
    is_paid,
    is_cancelled
from {{ ref('int_order_latest_state') }}
