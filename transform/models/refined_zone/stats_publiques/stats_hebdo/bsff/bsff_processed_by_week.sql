{{
    config(
        indexes = [ {'columns': ['semaine'], 'unique': True }]
    )
}}

{{ create_bsff_counts("operation_date", "traitements", "quantite_traitee") }}
