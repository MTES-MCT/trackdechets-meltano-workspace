{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsff","destination_reception_date", "receptions", "quantite_recue") }}
