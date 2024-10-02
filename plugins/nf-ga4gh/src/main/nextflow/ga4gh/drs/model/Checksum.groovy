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


