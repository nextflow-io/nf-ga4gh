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


