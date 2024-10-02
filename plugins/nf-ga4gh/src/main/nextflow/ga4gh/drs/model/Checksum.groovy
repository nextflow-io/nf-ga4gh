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

import java.io.File
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import com.twmacinta.util.MD5

/**
 * Define the Checksum object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class Checksum {

    String checksum
    String type = "md5"

    /**
     * Construct a DRS object.
     *
     * @param sourceFile The path to the source of the published file
     */
    Checksum(File sourceFile) {
        this.checksum = MD5.asHex(MD5.getHash(sourceFile))
    }

    /**
     * Transform the object to a map
     *
     */
    public Map toMap() {
        return [
            checksum: this.checksum,
            type: this.type
        ]
    }

}


