{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsdasri","transporter_taken_over_at", "envois", "quantite_envoyee") }}
