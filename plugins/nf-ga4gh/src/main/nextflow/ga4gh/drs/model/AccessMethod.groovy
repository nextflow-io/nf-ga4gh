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
import java.nio.file.Path
import java.util.UUID

import nextflow.ga4gh.drs.config.DrsConfig

/**
 * Define the AccessMethod object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class AccessMethod {

    String type
    AccessUrl accessUrl
    String accessId
    String region = ""

    /**
     * Construct a DRS object.
     *
     * @param destPath The URL or path to the destination of the published file
     * @param config The DrsConfig object
     */
    AccessMethod(Path destPath, DrsConfig config) {
        def String protocol = destPath.toUri().toString().split("://")[0]
        if(["s3", "gs", "ftp", "https"].contains(protocol)) {
            // No support for 'gsiftp', 'globus' and 'htsget', implement this later if needed
            this.type = protocol
        } else {
            this.type = "file"
        }
        this.accessUrl = new AccessUrl(destPath, this.type, config)
        this.accessId = UUID.randomUUID().toString()
    }

    /**
     * Transform the object to a map
     *
     */
    public Map toMap() {
        return [
            type: this.type,
            access_url: this.accessUrl.toMap(),
            access_id: this.accessId,
            region: this.region
        ]
    }

}


