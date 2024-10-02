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
