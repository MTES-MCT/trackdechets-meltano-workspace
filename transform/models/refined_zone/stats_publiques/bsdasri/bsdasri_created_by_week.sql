{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsdasri","created_at", "creations", "quantite_tracee") }}
