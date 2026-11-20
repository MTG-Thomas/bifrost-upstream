"""RFC 8693 values shared by the token endpoint and the workload CLI."""

from __future__ import annotations

TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
SUBJECT_TOKEN_ID = "urn:ietf:params:oauth:token-type:id_token"
SUBJECT_TOKEN_JWT = "urn:ietf:params:oauth:token-type:jwt"
ISSUED_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"
WORKLOAD_SCOPE = "bifrost:api"
WORKLOAD_CREDENTIAL_KIND = "workload"

SUPPORTED_SUBJECT_TOKEN_TYPES = frozenset({SUBJECT_TOKEN_ID, SUBJECT_TOKEN_JWT})

DEFAULT_LIFETIME_SECONDS = 900
MIN_LIFETIME_SECONDS = 60
MAX_LIFETIME_SECONDS = 3600
MAX_RULES = 100
CLOCK_SKEW_SECONDS = 60
MAX_SUBJECT_TOKEN_CHARS = 16_384
JWKS_CACHE_SECONDS = 600
JWKS_TIMEOUT_SECONDS = 5.0
JWKS_MAX_BYTES = 65_536
MAX_AUDIENCES = 4
MAX_CONDITIONS = 8
MAX_PINNED_KEYS = 8

ALLOWED_ALGS = frozenset(
    {
        "RS256",
        "RS384",
        "RS512",
        "PS256",
        "PS384",
        "PS512",
        "ES256",
        "ES384",
        "ES512",
    }
)

RESERVED_CONDITION_CLAIMS = frozenset({"iss", "aud", "exp", "iat", "nbf", "jti"})

GITHUB_ISSUER = "https://token.actions.githubusercontent.com"
GITHUB_JWKS_URI = "https://token.actions.githubusercontent.com/.well-known/jwks"
GITHUB_PROVIDER = "github_actions"
AZURE_PROVIDER = "azure_managed_identity"
