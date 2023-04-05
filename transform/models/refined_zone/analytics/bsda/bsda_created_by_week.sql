{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsda","created_at", "created") }}
