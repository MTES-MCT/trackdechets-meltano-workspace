{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsdasri","emitter_emission_signature_date", "emissions", "quantite_emise") }}
