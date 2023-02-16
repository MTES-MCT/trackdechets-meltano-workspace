{{
    config(
        indexes = [ {'columns': ['week'] }]
    )
}}

{{ create_bordereaux_counts("bsdd","sent_at", "sent") }}
