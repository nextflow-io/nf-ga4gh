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


