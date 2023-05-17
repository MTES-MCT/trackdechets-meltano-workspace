{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsff_packaging","operation_date", "packagings_processed", "packagings_quantity_processed") }}
