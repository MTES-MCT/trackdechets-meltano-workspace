{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsff_packaging","operation_date", "contenants_traites", "quantite_traitee") }}
