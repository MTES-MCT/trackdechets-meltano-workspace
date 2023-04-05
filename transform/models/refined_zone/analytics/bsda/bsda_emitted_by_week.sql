{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsda","emitter_emission_signature_date", "emitted") }}
