{{
    config(
        indexes = [ {'columns': ['week'], 'unique': True }]
    )
}}

{{ create_bordereaux_counts("bsvhu","transporter_transport_taken_over_at", "sent") }}
