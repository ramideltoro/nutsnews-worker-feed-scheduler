# Container Access

Image: `ghcr.io/ramideltoro/nutsnews-worker-feed-scheduler:<commit-sha>`

The publish workflow validates the service, builds a non-root multi-stage image, produces build provenance/SBOM metadata, signs the image with keyless cosign, and pushes immutable SHA-tagged images. It does not publish mutable `latest` tags.

The intended production package consumer is:

```text
ramideltoro/nutsnews-backend/.github/workflows/protected-backend-ansible-apply.yml
```

That workflow runs in the protected `production-backend` environment and must use a short-lived `GITHUB_TOKEN` with `packages: read`. App repositories must not receive production deployment secrets.
