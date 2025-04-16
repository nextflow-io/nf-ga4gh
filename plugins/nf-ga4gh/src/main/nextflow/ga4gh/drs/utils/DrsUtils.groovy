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
package nextflow.ga4gh.drs.utils

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

/**
 * Some common DRS utils
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class DrsUtils {

    /**
     * Get the sample name from the file name
     *
     * @param file the file from which to derive the samplename
     */
    public String getSampleName(Path file) {
        def String baseName = file.toString().split("/")[-1]

        // TODO This should be improved to work for every occasion (not only ours)
        def Pattern familyPattern = Pattern.compile(/^(Proband_\d+[_\.]\d+).*$/, Pattern.MULTILINE)
        def Matcher familyMatcher = familyPattern.matcher(baseName)
        if(familyMatcher.find()) {
            return familyMatcher.group(1)
        }

        def Pattern samplePattern = Pattern.compile(/^((K|FD|D|I|DNA)\d+[A-Z]?).*$/, Pattern.MULTILINE)
        def Matcher sampleMatcher = samplePattern.matcher(baseName)
        if(sampleMatcher.find()) {
            return sampleMatcher.group(1)
        }

        def Pattern otherPattern = Pattern.compile(/^([^-_\.]+).*$/, Pattern.MULTILINE)
        def Matcher otherMatcher = otherPattern.matcher(baseName)
        if(otherMatcher.find()) {
            return otherMatcher.group(1)
        }

        throw new DrsObjectCreationException("Unable to parse the sample name from ${baseName}")
    }

}


