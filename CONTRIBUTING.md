# nf-ga4gh

Contributions are welcome. Fork [this repository](https://github.com/nextflow-io/nf-ga4gh) and open a pull request to propose changes. Consider submitting an [issue](https://github.com/nextflow-io/nf-ga4gh/issues/new) to discuss any proposed changes with the maintainers before submitting a pull request.

## Development

Build and install the plugin to your local Nextflow installation:

```bash
make install
```

Run with Nextflow as usual:

```bash
nextflow run seqeralabs/nf-canary -plugins nf-ga4gh@<version>
```

## Pull Requests

Community contributions should adhere to the following standards:

- Every change should be proposed through a **pull request (PR)**

- All functionality (e.g. config options) should be **documented** in the README (or the `docs` folder if needed)

- Every PR must be **approved** by at least one maintainer or a delegated reviewer

- Every PR must **pass** the required CI checks, which include building the plugin, unit tests, and an end-to-end test with a Nextflow pipeline

- PRs should be **squashed** when merged to `main`

- Community PRs should **not** update the version file -- the version should only be updated in a separate release commit when making a release

## Publishing

The plugin follows semantic versioning -- `major.minor.patch`, where `major` is bumped for breaking changes, `minor` is bumped for new functionality, and `patch` is bumped for bug fixes.

In practice, it is acceptable to bump only the minor version for small breaking changes (e.g. removing an old feature). Users can pin a minor version in their config to receive the latest patch release without breaking changes:

```groovy
plugins {
    // use the latest 1.4.x release
    id 'nf-ga4gh@~1.4.0'
}
```

To publish a new plugin version:

1. Update the [version file](./VERSION) and make a release commit (e.g. "Release 1.4.0").

2. Run `make release` to build and publish the plugin.

3. Make a [GitHub release](https://github.com/nextflow-io/nf-ga4gh/releases).

See the [Nextflow documentation](https://docs.seqera.io/nextflow/plugins/plugin-registry) to learn how to authenticate with the Nextflow registry. Currently, only one person at a time can "own" the plugin and publish new versions.
