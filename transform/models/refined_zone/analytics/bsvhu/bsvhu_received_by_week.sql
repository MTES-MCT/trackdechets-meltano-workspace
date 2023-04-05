{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsvhu","destination_reception_date", "received") }}
