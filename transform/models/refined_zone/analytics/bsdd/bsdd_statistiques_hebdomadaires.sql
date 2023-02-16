SELECT
    *
FROM
    {{ ref('bsdd_created_by_week') }}
    full OUTER JOIN {{ ref('bsdd_emitted_by_week') }} using ("week")
    full OUTER JOIN {{ ref('bsdd_sent_by_week') }} using ("week")
    full OUTER JOIN {{ ref('bsdd_received_by_week') }} using ("week")
    