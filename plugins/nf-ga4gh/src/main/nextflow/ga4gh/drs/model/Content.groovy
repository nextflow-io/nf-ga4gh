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

/**
 * Define the Content object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class Content {

    String name
    String id
    List<String> drsUri
    List<String> contents

    /**
     * Construct a Content object.
     *
     */
    Content() {
        // This has not been implemented yet
    }

    /**
     * Transform the object to a map
     *
     */
    public Map toMap() {
        return null
    }

}


