{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsvhu","created_at", "creations", "quantite_tracee") }}
