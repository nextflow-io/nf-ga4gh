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


