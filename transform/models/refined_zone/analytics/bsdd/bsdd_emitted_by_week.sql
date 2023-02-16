{{
    config(
        indexes = [ {'columns': ['week'] }]
    )
}}

{{ create_bordereaux_counts("bsdd","emitted_at", "emitted") }}
