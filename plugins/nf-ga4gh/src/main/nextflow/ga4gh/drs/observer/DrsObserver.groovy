/*
 * Copyright (c) 2024 Nicolas Vannieuwkerke
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
package nextflow.ga4gh.drs.observer

import java.nio.file.Path
import java.nio.file.Files
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import groovy.yaml.YamlBuilder
import org.yaml.snakeyaml.Yaml

import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.Nextflow

import nextflow.ga4gh.drs.config.DrsConfig
import nextflow.ga4gh.drs.client.DrsClient
import nextflow.ga4gh.drs.model.DrsObject

/**
 * Implement the trace observer functions.
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class DrsObserver implements TraceObserver {

    private DrsConfig config
    private DrsClient client
    private List<Map> idFileList = []

    /**
     * Initialize the configuration and DRS client on flow creation
     *
     * @param session The current Nextflow session.
     */
    @Override
    void onFlowCreate(Session session) {
        this.config = new DrsConfig(session.config)
        this.client = new DrsClient(this.config)
    }

    /**
     * Create a DRS object of the published file and publish the object
     *
     * @param destination The destination of the published file.
     * @param source The source of the published file.
     */
    @Override
    void onFilePublish(Path destination, Path source) {
        if(new File(source.toString()).isDirectory()) {
            publishFilesFromDir(destination, source)
        }
        else {
            createAndPublishObject(destination, source)
        }
    }

    public void publishFilesFromDir(Path destination, Path directory) {
        new File(directory.toString()).listFiles().each {
            Path file = Nextflow.file(it) as Path
            Path newDestination = destination.resolve(file.name)
            if(it.isDirectory()) {
                publishFilesFromDir(newDestination, file)
            } else {
                createAndPublishObject(newDestination, file)
            }
        }
    }

    void createAndPublishObject(Path destination, Path source) {
        if(!checkIfFileHasExtension(destination, this.config.allowedExtensions)) {
            return
        }
        DrsObject obj = new DrsObject(destination, source, this.config)
        String id = this.client.uploadObject(obj.toMap())
        this.idFileList.add(["drs_id": id, "file": destination.toUri().toString().replaceFirst("///", "//")])
    }

    /**
     * Create a summary file of all published DRS objects
     *
     */
    @Override
    void onFlowComplete() {
        // Don't create a summary file if no path has been given
        if(!this.config.summary) {
            return
        }
        def Path summaryFile = (Path) Nextflow.file(this.config.summary)
        def String summaryExtension = this.config.summary.tokenize('.').last()
        def String fileText
        switch(summaryExtension) {
            case "csv":
                fileText = "drs_id,file\n"
                this.idFileList.each { entry ->
                    fileText += "${entry.drs_id},${entry.file}\n".toString()
                }
                break
            case "tsv":
                fileText = "drs_id\tfile\n"
                this.idFileList.each { entry ->
                    fileText += "${entry.drs_id}\t${entry.file}\n".toString()
                }
                break
            case "json":
                fileText = new JsonBuilder(this.idFileList).toPrettyString()
                break
            case ["yml", "yaml"]:
                fileText = new YamlBuilder()(this.idFileList).toString()
                break
        }
        summaryFile.text = fileText
        log.debug("Created DRS summary file at ${this.config.summary}")
    }

    private static Boolean checkIfFileHasExtension(Path file, List<String> extensions) {
        if(extensions) {
            def String destString = file.toString()
            for(ext : extensions) {
                if(destString.endsWith(ext)) { return true }
            }
            return false
        }
        return true
    }

}
