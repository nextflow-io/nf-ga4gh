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
package nextflow.ga4gh.drs.model

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import java.util.UUID
import java.nio.file.Path
import java.nio.file.Files
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.lang.IllegalStateException

import nextflow.Nextflow

import nextflow.ga4gh.drs.exceptions.DrsObjectCreationException
import nextflow.ga4gh.drs.config.DrsConfig
import nextflow.ga4gh.drs.utils.DrsUtils

/**
 * Define the DRS object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class DrsObject {

    String id
    String name
    String selfUri
    long size
    String createdTime
    String updatedTime
    String version = "1"
    String mimeType = "application/json"
    List<Checksum> checksums
    List<AccessMethod> accessMethods
    List<Content> contents
    String description = null
    List<String> aliases

    /**
     * Construct a DRS object.
     *
     * @param destPath The URL or path to the destination of the published file
     * @param sourcePath The path to the source of the published file
     * @param config The DrsConfig object
     */
    DrsObject(Path destPath, Path sourcePath, DrsConfig config) {
        def File sourceFile = sourcePath.toFile()
        this.id = UUID.randomUUID().toString()
        log.debug("Creating DRS object '${this.id}' for file '${destPath.toUri().toString()}'")

        this.name = new DrsUtils().getSampleName(sourcePath)
        this.selfUri = "drs://${config.endpointNoProtocol}/${this.id}".toString()
        this.size = sourceFile.length()
        this.createdTime = Files.getAttribute(sourcePath, "creationTime") as String
        this.updatedTime = Files.getAttribute(sourcePath, "lastModifiedTime") as String
        this.checksums = [new Checksum(sourceFile)]
        Content content = new Content()
        this.contents = content.toMap() ? [content] : null
        AccessMethod accessMethod = new AccessMethod(destPath, config)
        this.accessMethods = accessMethod.toMap() ? [accessMethod] : null

        // TODO improve alias handling
        this.aliases = [
            "${config.run}/${this.name}" as String
        ]
    }

    /**
     * Transform the object to a map
     *
     */
    public Map toMap() {
        return [
            id: this.id,
            name: this.name,
            self_uri: this.selfUri,
            size: this.size,
            created_time: this.createdTime,
            updated_time: this.updatedTime,
            version: this.version,
            mime_type: this.mimeType,
            checksums: this.checksums.collect { it.toMap() },
            access_methods: this.accessMethods?.collect { it.toMap() },
            contents: this.contents?.collect { it.toMap() },
            description: this.description,
            aliases: this.aliases
        ]
    }

}


