{{
    config(
        indexes = [ {'columns': ['week'] }]
    )
}}

{{create_bordereaux_counts("bsdd","received_at", "received")}}