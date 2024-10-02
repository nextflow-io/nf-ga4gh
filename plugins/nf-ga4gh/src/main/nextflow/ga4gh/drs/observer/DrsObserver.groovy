/*
 * MIT License
 *
 * Copyright (c) 2024 Nicolas Vannieuwkerke
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next
 * paragraph) shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
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
