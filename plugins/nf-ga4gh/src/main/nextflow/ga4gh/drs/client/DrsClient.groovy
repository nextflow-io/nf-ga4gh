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
package nextflow.ga4gh.drs.client

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder

import java.time.Instant
import java.lang.Long
import java.io.File
import java.io.IOException
import java.io.OutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Path
import java.nio.channels.Channels
import java.nio.channels.Pipe
import java.nio.charset.StandardCharsets

import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.entity.mime.content.StringBody

import nextflow.ga4gh.drs.exceptions.DrsObjectPublishingException
import nextflow.ga4gh.drs.exceptions.DrsAuthenticationException
import nextflow.ga4gh.drs.config.DrsConfig
import nextflow.ga4gh.drs.utils.DrsUtils

/**
 * Define the DRS client
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@Slf4j
@CompileStatic
class DrsClient {

    String authHeader
    long authExpiration

    String user
    String password
    String endpoint

    /**
     * Create a DRS client based on a DrsConfig object
     *
     * @param config The DrsConfig object.
     */
    DrsClient(DrsConfig config) {
        this.user = config.user
        this.password = config.password
        this.endpoint = config.endpoint
        refreshToken()
    }

    /**
     * Check if the token is expired or expires soon. If so, the token should be refreshed
     *
     */
    private void checkToken() {
        long epoch = Instant.now().toEpochMilli()
        // Refresh the token if it expires in less than one minute
        if(authExpiration - epoch < 1*60*100) {
            refreshToken()
            log.debug("Refreshed DRS token")
        }
    }

    /**
     * Refresh the DRS token
     *
     */
    private void refreshToken() {
        // Create the form with the username and password
        HttpEntity httpEntity = MultipartEntityBuilder.create()
                .addPart(
                    "username",
                    new StringBody(
                        this.user,
                        ContentType.create("application/x-www-form-urlencoded", StandardCharsets.UTF_8)
                    )
                )
                .addPart(
                    "password",
                    new StringBody(
                        this.password,
                        ContentType.create("application/x-www-form-urlencoded", StandardCharsets.UTF_8)
                    )
                )
                .build()

        // Efficiently stream the form
        Pipe pipe = Pipe.open()
        new Thread(() -> {
            try (OutputStream outputStream = Channels.newOutputStream(pipe.sink())) {
                httpEntity.writeTo(outputStream)
            }
        }).start()

        // Do a POST request to get the bearer token and add it the a header value
        HttpClient httpClient = HttpClient.newHttpClient()

        HttpRequest request = HttpRequest.newBuilder(new URI("${this.endpoint}/token".toString()))
                .header("Content-Type", httpEntity.getContentType().getValue())
                .version(HttpClient.Version.HTTP_1_1)
                .POST(BodyPublishers.ofInputStream(() -> Channels.newInputStream(pipe.source()))).build()

        HttpResponse<String> responseBody = httpClient.send(request, BodyHandlers.ofString(StandardCharsets.UTF_8))

        Map responseMap = (Map) new JsonSlurper().parseText(responseBody.body())
        if(responseMap.containsKey("status_code") && responseMap.status_code != 200) {
            throw new DrsAuthenticationException(responseMap.msg.toString())
        }
        this.authHeader = "Bearer ${responseMap.access_token}"
        this.authExpiration = Instant.now().toEpochMilli() + Long.parseLong(responseMap.expires_in.toString()) * 60 * 100
    }

    /**
     * Upload a DRS object
     *
     * @param obj A Groovy map with the structure of a DRS object
     */
    public String uploadObject(Map obj) {
        checkToken()

        // Upload the DRS Object using a POST request
        HttpClient client = HttpClient.newHttpClient()
        String objBody = new JsonBuilder(obj).toPrettyString()
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("${this.endpoint}/ga4gh/drs/v1/objects"))
            .header("Authorization", this.authHeader)
            .header("Content-Type", "application/json")
            .version(HttpClient.Version.HTTP_1_1)
            .POST(HttpRequest.BodyPublishers.ofString(objBody))
            .build()

        HttpResponse<String> responseBody = client.send(request, BodyHandlers.ofString(StandardCharsets.UTF_8))

        Map responseMap = (Map) new JsonSlurper().parseText(responseBody.body())
        Integer statusCode = responseBody.statusCode()

        // Return the DRS id if the object has been created or when the object exists
        switch(statusCode) {
            case 201:
                return responseMap.object_id
            case 409:
                List accessMethods = obj.access_methods as List
                String url = accessMethods[0]['access_url']['url']
                log.debug("DRS object for '${url}' already exists. Skipping the upload of this object")
                return getIdFromUrl(url)
            default:
                throw new DrsObjectPublishingException("Received an error when publishing a DRS object (Status code: ${statusCode}): ${responseBody.body()}")
        }
    }

    /**
     * Get a DRS id based on a publish URL
     *
     * @param url The URL to get the DRS id from
     */
    public String getIdFromUrl(String url) {
        checkToken()
        String sample = new DrsUtils().getSampleName(url as Path)

        HttpClient client = HttpClient.newHttpClient()
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("${this.endpoint}/ga4gh/drs/v1/objects?alias=${sample}"))
            .header("Authorization", this.authHeader)
            .version(HttpClient.Version.HTTP_1_1)
            .GET()
            .build()

        HttpResponse<String> responseBody = client.send(request, BodyHandlers.ofString(StandardCharsets.UTF_8))

        List responseObjects = (List) new JsonSlurper().parseText(responseBody.body())
        Integer statusCode = responseBody.statusCode()

        Map correctObj = (Map) responseObjects.find { obj -> 
            List urls = obj['access_methods']['access_url']['url'] as List
            return urls.contains(url)
        }
        if(correctObj) {
            return correctObj.id
        }
        throw new DrsObjectPublishingException("Creating a DRS object for ${url} returned a 'object already exists' response, but the object couldn't be found with an alias search on '${sample}'")
    }

}
