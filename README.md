# GA4GH plugin for Nextflow
 
## Summary
 
This plugin implements the support for GA4GH APIs for Nextflow.

It currently supports the [Task Execution Service (TES) API](https://github.com/ga4gh/task-execution-schemas) and the [Data Repository Service (DRS) API](https://github.com/ga4gh/data-repository-service-schemas).

The [Task Execution Schema](https://github.com/ga4gh/task-execution-schemas) (TES) project by the [GA4GH](https://www.ga4gh.org) standardization initiative is an effort to define a standardized schema and API for describing batch execution tasks in a portable manner.

The [Data Repository Service](https://github.com/ga4gh/data-repository-service-schemas) (DRS) API by the [GA4GH](https://www.ga4gh.org) standardization initiative provides a generic interface for retrieving scientific data objects (genomic files, etc.) via a persistent, resolvable identifier, regardless of which repository hosts them.

## Get Started

To use this plugin, add it to your `nextflow.config`:

```groovy
plugins {
    id 'nf-ga4gh'
}
```

Configure the TES executor:

```groovy
process.executor = 'tes'
```

> [!NOTE]
>
> While the TES API is designed to abstract workflow managers from direct storage access, Nextflow still needs to access the shared work directory used by your TES endpoint.
> 
> For example, if your TES endpoint is located in Azure and uses Azure Blob storage to store the work directory, you still need to provide the necessary Azure credentials for Nextflow to access the Blob storage.

## Configuration

```groovy
plugins {
    id 'nf-ga4gh'
}

process {
    executor = 'tes'
    container = 'quay.io/nextflow/bash'
}

tes {
    // See `Authentication` section
    endpoint = 'http://localhost:8000'

    // Connect/read/write timeout duration (in seconds) to TES endpoint. Default 10s.
    timeout = 30

    // Override the default task polling interval (5s)
    pollInterval = '10s'

    // TES task tags
    tags {
        tag1 = 'abc'
        tag2 = 'xyz'
    }
}
```

### Endpoint

> [!TIP]
>
> It is important that the endpoint is specified without the trailing slash; otherwise, the resulting URLs will not be normalized and the requests to TES will fail.

The default endpoint is `http://localhost:8000`

```groovy
tes {
    endpoint = '<endpoint>'
}
```

### Authentication

The TES API supports multiple forms of authentication:

#### Basic

```groovy
tes {
    basicUsername = '<username>'
    basicPassword = '<password>'
}
```

#### API key

```groovy
tes {
    apiKeyParamMode = '<mode>' // 'query' or 'header'
    apiKeyParamName = '<param-name>'
    apiKey = '<key>'
}
```

#### OAuth

```groovy
tes {
    oauthToken = '<token>'
}
```

## TES Server

You can deploy a local [Funnel](https://ohsu-comp-bio.github.io/funnel) server using the following commands:

```bash
curl -fsSL https://ohsu-comp-bio.github.io/funnel/install.sh | bash

funnel server run
```

## DRS (Data Repository Service)

`drs://` URIs can be used directly as process inputs or outputs, without any extra configuration:

```groovy
workflow {
    align(file("drs://drs.example.org/object-id"))
}
```

Under the TES executor, the `drs://` URI is passed straight through to the TES backend as the task input `url` — resolution is left entirely to the TES server. For the local executor (or when Nextflow itself needs to read a DRS object, e.g. staging into a shared work directory), the plugin resolves the URI via the standard two-hop DRS protocol and streams the object over HTTPS.

### Configuration

```groovy
drs {
    // Bearer token sent on every DRS request. Takes precedence over
    // username/password if both are set.
    accessToken = '<token>'

    // HTTP Basic auth credentials, for DRS servers that gate access this way
    // instead of (or in addition to) a bearer token.
    username = '<username>'
    password = '<password>'

    // Scheme used to contact the DRS metadata API. Defaults to 'https';
    // set to 'http' to target a plain-HTTP DRS server (e.g. local development).
    metadataScheme = 'https'
}
```

> [!NOTE]
>
> The DRS filesystem is read-only — write operations (`newOutputStream`, `delete`, `copy`, `move`, etc.) are not supported.

## Resources

- [GA4GH Homepage](https://www.ga4gh.org)
- [Task Execution Service (TES)](https://www.ga4gh.org/product/task-execution-service-tes)
- [Data Repository Service (DRS)](https://github.com/ga4gh/data-repository-service-schemas)

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
