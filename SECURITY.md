# Security Policy

Kron is alpha software.

## Supported Versions

Security fixes currently target the latest `main` branch until the first published release.

## Reporting a Vulnerability

If you find a security issue, please do not open a public issue with exploit details. Contact the maintainer through the repository security advisory flow once the GitHub repository is created.

## Security Notes

- Embedded mode stores state locally in the configured data directory.
- Local IPC uses a token, but it is not an enterprise authentication system.
- Server mode is experimental and should not be exposed directly to the public internet.
- Scheduled callbacks can perform real side effects, so application code should use idempotency keys and least-privilege credentials.
