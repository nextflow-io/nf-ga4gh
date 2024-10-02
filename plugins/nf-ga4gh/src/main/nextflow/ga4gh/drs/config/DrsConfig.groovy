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
package nextflow.ga4gh.drs.config

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.ClientConfiguration

import nextflow.ga4gh.drs.exceptions.DrsConfigException
import nextflow.cloud.aws.AwsClientFactory
import nextflow.cloud.aws.config.AwsConfig

/**
 * Define the plugin configuration values.
 *
 * The configuration values can be extracted from the map and will be stored as
 * on the instance.
 *
 *
 * TODO: Describe the configuration of your actual implementation.
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class DrsConfig {

    Boolean enabled
    String endpoint
    String endpointNoProtocol
    String user
    String password
    AmazonS3 s3Client
    List<String> allowedExtensions
    String run
    String summary

    /**
     * Construct a configuration instance.
     *
     * @param map A nextflow plugin wrapper instance.
     */
    DrsConfig(Map map = [:]) {
        this.enabled = map.navigate("drs.enabled") ?: false
        this.endpoint = map.navigate("drs.endpoint") ?: System.getenv("DRS_URL") ?: ""
        this.endpointNoProtocol = this.endpoint.split("://")[-1]
        this.user = map.navigate("drs.user") ?: System.getenv("DRS_USERNAME") ?: ""
        this.password = map.navigate("drs.password") ?: System.getenv("DRS_PASSWORD") ?: ""
        Map awsConfig = (Map) map.navigate("aws") ?: [:]
        awsConfig.region = awsConfig.region ?: "uz"
        this.s3Client = new AwsClientFactory(new AwsConfig(awsConfig)).getS3Client()
        this.allowedExtensions = map.navigate("drs.allowedExtensions") as List<String> ?: []
        this.run = map.navigate("drs.run") ?: "" // TODO implement a more dynamic way for pipeline runs with more than one sequencer run
        this.summary = map.navigate("drs.summary") ?: ""

        // Some failsafe options to prevent weird errors
        if(!this.enabled) { return }

        if(!this.endpoint) {
            throw new DrsConfigException("Please provide a DRS endpoint with the drs.endpoint configuration option or with the DRS_URL environment variable")
        }

        if(!this.user) {
            throw new DrsConfigException("Unable to get the DRS username. Make sure the drs.user configuration option or the DRS_USERNAME environment variable is set")
        }

        if(!this.password) {
            throw new DrsConfigException("Unable to get the DRS password. Make sure the drs.password configuration option or the DRS_PASSWORD environment variable is set")
        }

        if(this.summary) {
            String summaryExtension = this.summary.tokenize(".").last() ?: ""
            List allowedSummaryExtensions = ["csv", "tsv", "json", "yaml", "yml"]
            if(!allowedSummaryExtensions.contains(summaryExtension)) {
                throw new DrsConfigException("Unrecognized extension used for the DRS summary file. The extension (${summaryExtension}) should be on of these: ${allowedSummaryExtensions.join(',')}")
            }
        }

        if(!this.run) {
            throw new DrsConfigException("Please provide a run with the `drs.run` configuration option")
        }

    }

}
