{{
    config(
        indexes = [ {'columns': ['week'] }]
    )
}}

{{ create_bordereaux_counts("bsdd","created_at", "created") }}
