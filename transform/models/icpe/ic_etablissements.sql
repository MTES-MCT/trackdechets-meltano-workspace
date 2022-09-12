select
    "codeS3ic",
    "s3icNumeroSiret"
from
    raw_zone.ic_etablissement ie
where
    ie."s3icNumeroSiret" is null
    or
    LENGTH(ie."s3icNumeroSiret")= 14