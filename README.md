# GA4GH plugin for Nextflow
 
## Summary
 
This plugin implements the support for GA4GH APIs for Nextflow.

It currently only supports the [Task Execution Service (TES) API](https://github.com/ga4gh/task-execution-schemas).

The [Task Execution Schema](https://github.com/ga4gh/task-execution-schemas) (TES) project by the [GA4GH](https://www.ga4gh.org) standardization initiative is an effort to define a standardized schema and API for describing batch execution tasks in a portable manner.

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

## Examples

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

### TES Server

You can deploy a local [Funnel](https://ohsu-comp-bio.github.io/funnel) server using the following commands:

```bash
curl -fsSL https://ohsu-comp-bio.github.io/funnel/install.sh | bash

funnel server run
```

## Resources

- [GA4GH Homepage](https://www.ga4gh.org)
- [Task Execution Service (TES)](https://www.ga4gh.org/product/task-execution-service-tes)

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
