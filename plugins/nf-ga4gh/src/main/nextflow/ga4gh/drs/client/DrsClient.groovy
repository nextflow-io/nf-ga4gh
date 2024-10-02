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
