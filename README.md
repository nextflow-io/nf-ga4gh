# nf-ga4gh plugin 
 
This plugin implements the support for GA4GH APIs for Nextflow. Currently only supports the [Task Execution Service (TES) API](https://github.com/ga4gh/task-execution-schemas).

See the [Nextflow documentation](https://nextflow.io/docs/latest/plugins.html) for more information about developing plugins.

## Configuration

The [Task Execution Schema](https://github.com/ga4gh/task-execution-schemas) (TES) project by the [GA4GH](https://www.ga4gh.org) standardization initiative is an effort to define a standardized schema and API for describing batch execution tasks in a portable manner.

Nextflow supports the TES API via the `tes` executor, which allows the submission of workflow tasks to a remote execution backend exposing a TES API endpoint.

The pipeline processes must specify the Docker image to use by defining the `container` directive, either in the pipeline script or the `nextflow.config` file. Additionally, the pipeline work directory must be accessible to the TES backend.

To enable this executor, add the following settings to your Nextflow configuration:

```groovy
plugins {
  id 'nf-ga4gh'
}

process.executor = 'tes'
tes.endpoint = '<endpoint>'
```

The default endpoint is `http://localhost:8000`. It is important that the endpoint is specified without the trailing slash; otherwise, the resulting URLs will not be normalized and the requests to TES will fail.

The TES API supports multiple forms of authentication:

```groovy
// basic
tes.basicUsername = '<username>'
tes.basicPassword = '<password>'

// API key
tes.apiKeyParamMode = '<mode>' // 'query' or 'header'
tes.apiKeyParamName = '<param-name>'
tes.apiKey = '<key>'

// OAuth
tes.oauthToken = '<token>'
```

:::{tip}
You can deploy a local [Funnel](https://ohsu-comp-bio.github.io/funnel/) server using the following command:

```bash
./funnel server --Server.HTTPPort 8000 --LocalStorage.AllowedDirs $HOME run
```
:::

:::{note}
While the TES API is designed to abstract workflow managers from direct storage access, Nextflow still needs to access the shared work directory used by your TES endpoint. For example, if your TES endpoint is located in Azure and uses Azure Blob storage to store the work directory, you still need to provide the necessary Azure credentials for Nextflow to access the Blob storage.
:::

## Plugin structure
                    
- `settings.gradle`
    
    Gradle project settings. 

- `plugins/nf-ga4gh`
    
    The plugin implementation base directory.

- `plugins/nf-ga4gh/build.gradle` 
    
    Plugin Gradle build file. Project dependencies should be added here.

- `plugins/nf-ga4gh/src/resources/META-INF/MANIFEST.MF` 
    
    Manifest file defining the plugin attributes e.g. name, version, etc. The attribute `Plugin-Class` declares the plugin main class. This class should extend the base class `nextflow.plugin.BasePlugin` e.g. `nextflow.hello.HelloPlugin`.

- `plugins/nf-ga4gh/src/resources/META-INF/extensions.idx`
    
    This file declares one or more extension classes provided by the plugin. Each line should contain the fully qualified name of a Java class that implements the `org.pf4j.ExtensionPoint` interface (or a sub-interface).

- `plugins/nf-ga4gh/src/main` 

    The plugin implementation sources.

- `plugins/nf-ga4gh/src/test` 

    The plugin unit tests. 


## Unit testing 

To run your unit tests, run the following command in the project root directory (ie. where the file `settings.gradle` is located):

```bash
./gradlew check
```

## Testing and debugging

To build and test the plugin during development, configure a local Nextflow build with the following steps:

1. Clone the Nextflow repository in your computer into a sibling directory:
    ```bash
    git clone --depth 1 https://github.com/nextflow-io/nextflow ../nextflow
    ```
  
2. Configure the plugin build to use the local Nextflow code:
    ```bash
    echo "includeBuild('../nextflow')" >> settings.gradle
    ```
  
   (Make sure to not add it more than once!)

3. Compile the plugin alongside the Nextflow code:
    ```bash
    make assemble
    ```

4. Run Nextflow with the plugin, using `./launch.sh` as a drop-in replacement for the `nextflow` command, and adding the option `-plugins nf-ga4gh` to load the plugin:
    ```bash
    ./launch.sh run nextflow-io/hello -plugins nf-ga4gh
    ```

## Testing without Nextflow build

The plugin can be tested without using a local Nextflow build using the following steps:

1. Build the plugin: `make buildPlugins`
2. Copy `build/plugins/<your-plugin>` to `$HOME/.nextflow/plugins`
3. Create a pipeline that uses your plugin and run it: `nextflow run ./my-pipeline-script.nf`

## Package, upload, and publish

The project should be hosted in a GitHub repository whose name matches the name of the plugin, that is the name of the directory in the `plugins` folder (e.g. `nf-ga4gh`).

Follow these steps to package, upload and publish the plugin:

1. Create a file named `gradle.properties` in the project root containing the following attributes (this file should not be committed to Git):

   * `github_organization`: the GitHub organisation where the plugin repository is hosted.
   * `github_username`: The GitHub username granting access to the plugin repository.
   * `github_access_token`: The GitHub access token required to upload and commit changes to the plugin repository.
   * `github_commit_email`: The email address associated with your GitHub account.

2. Use the following command to package and create a release for your plugin on GitHub:
    ```bash
    ./gradlew :plugins:nf-ga4gh:upload
    ```

3. Create a pull request against [nextflow-io/plugins](https://github.com/nextflow-io/plugins/blob/main/plugins.json) to make the plugin accessible to Nextflow.
