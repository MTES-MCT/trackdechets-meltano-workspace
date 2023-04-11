{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsvhu","emitter_emission_signature_date", "emitted","quantity_emitted") }}
