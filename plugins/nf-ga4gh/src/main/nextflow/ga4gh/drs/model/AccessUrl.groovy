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
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.lang.reflect.Method

import nextflow.cloud.aws.nio.S3Path

import nextflow.ga4gh.drs.exceptions.DrsObjectCreationException
import nextflow.ga4gh.drs.config.DrsConfig

/**
 * Define the AccessUrl object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
class AccessUrl {

    String url
    List<String> headers = []

    /**
     * Construct a DRS object.
     *
     * @param destPath The URL or path to the destination of the published file
     * @param type The type of the destination path (e.g s3, az, gc...)
     * @param config The DrsConfig object
     */
    AccessUrl(Path destPath, String type, DrsConfig config) {
        switch(type) {
            case "s3":
                this.url = config.s3Client.getResourceUrl(destPath.bucket, destPath.key)
                break
            default:
                this.url = destPath.toUri().toString()
                break
        }
    }

    /**
     * Transform the object to a map
     *
     */
    public Map toMap() {
        return [
            url: this.url,
            headers: this.headers
        ]
    }

}


