/*
 * Copyright 2013-2024, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.ga4gh.tes.executor

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.transform.TypeCheckingMode
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.executor.Executor
import nextflow.extension.FilesEx
import nextflow.ga4gh.tes.client.ApiClient
import nextflow.ga4gh.tes.client.api.TaskServiceApi
import nextflow.ga4gh.tes.client.auth.ApiKeyAuth
import nextflow.ga4gh.tes.client.auth.Authentication
import nextflow.ga4gh.tes.client.auth.HttpBasicAuth
import nextflow.ga4gh.tes.client.auth.OAuth
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

import java.nio.file.Path

/**
 * Experimental TES executor
 *
 * See https://github.com/ga4gh/task-execution-schemas/
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
@ServiceName('tes')
class TesExecutor extends Executor implements ExtensionPoint {

    private TaskServiceApi client

    /**
     * A path accessible to TES where executable scripts need to be uploaded
     */
    private Path remoteBinDir

    private List<Path> remoteBinFiles = []

    @Override
    protected void register() {
        super.register()
        uploadBinDir()
        final timeout = session.config.navigate('tes.timeout', 10) as int
        client = new TaskServiceApi( new ApiClient(
                basePath: getEndpoint(),
                debugging: log.isTraceEnabled(),
                authentications: getAuthentications()) )
        if (timeout) {
            client.apiClient.setConnectTimeout(timeout * 1000)
            client.apiClient.setReadTimeout(timeout * 1000)
            client.apiClient.setWriteTimeout(timeout * 1000)
        }
        log.info "Initialized TES executor > endpoint: ${getEndpoint()} -- timeout: ${client.apiClient.getConnectTimeout() / 1000}s"
    }

    protected String getDisplayName() {
        return "$name [${getEndpoint()}]"
    }

    TaskServiceApi getClient() {
        client
    }

    @PackageScope
    Path getRemoteBinDir() {
        remoteBinDir
    }

    @PackageScope
    List<Path> getRemoteBinFiles() {
        remoteBinFiles
    }

    protected void uploadBinDir() {
        /*
         * upload local binaries
         */
        if( session.binDir && !session.binDir.empty() && !session.disableRemoteBinDir ) {
            final tempBin = getTempDir()
            log.info "Uploading local `bin` scripts folder to ${tempBin.toUriString()}/bin"
            remoteBinDir = FilesEx.copyTo(session.binDir, tempBin)

            remoteBinFiles = []
            session.binDir.eachFileRecurse { file ->
                if( file.isDirectory() )
                    return
                remoteBinFiles << tempBin.resolve('bin').resolve(session.binDir.relativize(file).toString())
            }
        }
    }
    
    protected String getEndpoint() {
        final String result = session.config.navigate('tes.endpoint', 'http://localhost:8000') as String
        log.debug "[TES] endpoint=$result"
        return result
    }

    protected Map<String, Authentication> getAuthentications() {
        final Map<String, Authentication> result = [:]

        // basic
        final username = session.config.navigate('tes.basicUsername')
        final password = session.config.navigate('tes.basicPassword')
        if( username && password )
            result['basic'] = new HttpBasicAuth(username: username, password: password)

        // API key
        final apiKeyParamMode = session.config.navigate('tes.apiKeyParamMode', 'query') as String
        final apiKeyParamName = session.config.navigate('tes.apiKeyParamName') as String
        final apiKey = session.config.navigate('tes.apiKey') as String
        if( apiKeyParamName && apiKey ) {
            final auth = new ApiKeyAuth(apiKeyParamMode, apiKeyParamName)
            auth.setApiKey(apiKey)
            result['apikey'] = auth
        }

        // OAuth
        final oauthToken = session.config.navigate('tes.oauthToken')
        if( oauthToken )
            result['oauth'] = new OAuth(accessToken: oauthToken)

        log.debug "[TES] Authentication methods: ${result.keySet()}"
        return result
    }

    protected String getAzureStorageAccount() {
        final storageAccount = session.config.navigate('azure.storage.accountName')
        log.debug "[TES] Azure storage account = ${storageAccount}"
        return storageAccount
    }

    /**
     * If the session has a cloud bucket dir (e.g. az://inputs/nextflow/work) and a local
     * work dir, returns a map with `localBase` (String) and `azureUri` (String) so that
     * callers can map local paths into their Azure equivalents.
     */
    @PackageScope
    @groovy.transform.CompileStatic(groovy.transform.TypeCheckingMode.SKIP)
    Map<String, String> getAzureWorkDirMapping() {
        final storageAccount = getAzureStorageAccount()
        if( !storageAccount ) return null
        final bucketDir = session.bucketDir
        if( !bucketDir ) return null
        final bucketUri = bucketDir.toUriString()
        if( !bucketUri.startsWith('az://') ) return null
        final localWorkDir = session.workDir?.toString()
        if( !localWorkDir ) return null
        return [localBase: localWorkDir, azureUri: bucketUri]
    }

    protected Map<String,String> getTags() {
        // Explicitly convert the values to String to allow passing tags through environment
        // variables, e.g. `tes { tags { MY_VAR = "${MY_VAR}" } }`. Env var values are otherwise of
        // GString type and cause the error `class org.codehaus.groovy.runtime.GStringImpl cannot
        // be cast to class java.lang.String`.
        final Map<String,String> result = (session.config.navigate('tes.tags') as Map)
    ?.collectEntries { k, v -> [k.toString(), v.toString()] }
        return result
    }

    /**
     * @return {@code true} whenever the containerization is managed by the executor itself
     */
    boolean isContainerNative() {
        return true
    }

    /**
     * Create a a queue holder for this executor
     *
     * @return the task monitor instance
     */
    TaskMonitor createTaskMonitor() {
        final pollInterval = session.config.navigate('tes.pollInterval', '5s') as String;
        TaskMonitor tpm = TaskPollingMonitor.create(session, config, name, 5, Duration.of(pollInterval));
        log.debug "Initialized task polling monitor (polling interval: ${pollInterval})";
        return tpm;
    }

    /*
     * Prepare and launch the task in the underlying execution platform
     */
    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        assert task
        assert task.workDir
        log.debug "[TES] Launching process > ${task.name} -- work folder: ${task.workDir}"
        new TesTaskHandler(task, this)
    }

}
