# Security Policy

## Supported Versions

`fastapi-jobs` is in active alpha development (see the `Development Status :: 3 -
Alpha` classifier on PyPI). Only the latest released version is supported --
security fixes are not backported to older releases.

| Version | Supported |
| ------- | --------- |
| latest  | Yes       |
| older   | No        |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, use GitHub's private vulnerability reporting:

1. Go to the [Security tab](https://github.com/stackadnan/fastapi-jobs/security) of
   this repository.
2. Click **"Report a vulnerability"**.

This opens a private advisory visible only to the maintainer, so the issue can be
discussed and fixed before it's public.

If that option isn't available, open a regular issue asking to be contacted
privately, without including any details of the vulnerability itself.

## What to expect

This is a solo-maintained, alpha-stage project, so there's no guaranteed response
time or SLA. Reports will be acknowledged and investigated on a best-effort basis,
and a fix will be released as a new PyPI version once available -- CHANGELOG.md will
note it under a "Security" heading.
