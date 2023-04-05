SELECT *
FROM
    {{ ref('bsff_created_by_week') }}
FULL OUTER JOIN {{ ref('bsff_emitted_by_week') }} USING ("week")
FULL OUTER JOIN {{ ref('bsff_sent_by_week') }} USING ("week")
FULL OUTER JOIN {{ ref('bsff_received_by_week') }} USING ("week")
FULL OUTER JOIN {{ ref('bsff_packagings_processed_by_week') }} USING ("week")
