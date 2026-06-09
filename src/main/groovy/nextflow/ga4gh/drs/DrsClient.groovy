/*
 * Copyright 2013-2024, Seqera Labs
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

package nextflow.ga4gh.drs

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Minimal HTTP client for the GA4GH Data Repository Service (DRS) API.
 *
 * Resolves a {@code drs://host/object_id} URI to a concrete download URL
 * via the two-hop DRS protocol:
 * <ol>
 *   <li>{@code GET https://host/ga4gh/drs/v1/objects/{object_id}} —
 *       returns a DrsObject with an {@code access_methods} array.</li>
 *   <li>If the chosen access method has only an {@code access_id} (no
 *       inline {@code access_url}):
 *       {@code GET https://host/ga4gh/drs/v1/objects/{object_id}/access/{access_id}}
 *       — returns an {@code AccessURL} with the final URL and optional
 *       auth headers.</li>
 * </ol>
 *
 * Access method preference order: {@code https} > {@code s3} > {@code gs}
 * > {@code ftp} > any other type.
 *
 * @author nf-ga4gh contributors
 */
@Slf4j
@CompileStatic
class DrsClient {

    /** Ordered preference for access method types */
    static final List<String> ACCESS_TYPE_PREFERENCE = ['https', 's3', 'gs', 'ftp']

    private final String bearerToken

    /**
     * The URL scheme used to contact the DRS metadata API.
     * Production deployments always use {@code https}; set to {@code http}
     * in tests that stub with plain-HTTP servers (e.g. WireMock).
     */
    private final String metadataScheme

    DrsClient(String bearerToken = null, String metadataScheme = 'https') {
        this.bearerToken = bearerToken
        this.metadataScheme = metadataScheme
    }

    /**
     * Resolve a {@code drs://} URI to an {@link AccessUrl} containing the
     * concrete download URL and any required HTTP headers.
     *
     * @param drsUri  a URI with scheme {@code drs}, e.g. {@code drs://drs.example.org/314159}
     * @return        the resolved {@link AccessUrl}
     * @throws IllegalArgumentException if {@code drsUri} is not a {@code drs://} URI
     * @throws IOException              if the DRS server returns an error or no usable access method is found
     */
    AccessUrl resolve(URI drsUri) throws IOException {
        if (drsUri.scheme != 'drs')
            throw new IllegalArgumentException("Expected a drs:// URI, got: $drsUri")

        final host = drsUri.host
        final port = drsUri.port > 0 ? ":${drsUri.port}" : ''
        // DRS object IDs may contain path segments; strip the leading '/'
        final objectId = drsUri.path?.replaceFirst('^/', '') ?: ''

        final baseUrl = "${metadataScheme}://${host}${port}/ga4gh/drs/v1"

        log.debug "[DRS] Resolving object: $drsUri"

        // --- Hop 1: GET /objects/{object_id} ---
        final objectUrl = "${baseUrl}/objects/${objectId}"
        final drsObject = getJson(objectUrl)

        final accessMethods = drsObject.access_methods as List<Map>
        if (!accessMethods)
            throw new IOException("DRS object '${objectId}' at '${host}' has no access_methods")

        final chosen = chooseAccessMethod(accessMethods)
        log.debug "[DRS] Chose access method: type=${chosen.type}"

        // --- Hop 2 (optional): GET /objects/{object_id}/access/{access_id} ---
        // When an access_id is present it always means a second-hop presigned URL is
        // required — even if access_url is also set (some servers set access_url to an
        // s3:// object key rather than an HTTP URL).  Prefer the access_id path so we
        // always end up with an HTTP(S) URL we can open.
        final accessId = chosen.access_id as String
        if (!accessId) {
            // No access_id → the inline access_url must be an HTTP(S) URL we can open directly
            final inlineUrl = chosen.access_url as Map
            if (!inlineUrl?.url)
                throw new IOException("DRS access method for object '${objectId}' has neither access_url nor access_id")
            final headers = (inlineUrl.headers as Map<String, String>) ?: Collections.<String, String>emptyMap()
            return new AccessUrl(url: inlineUrl.url as String, headers: headers)
        }

        final accessUrl = "${baseUrl}/objects/${objectId}/access/${accessId}"
        log.debug "[DRS] Fetching access URL via access_id: $accessId"
        final accessResponse = getJson(accessUrl)

        final url = accessResponse.url as String
        if (!url)
            throw new IOException("DRS access endpoint returned no URL for object '${objectId}', access_id '${accessId}'")

        final headers = (accessResponse.headers as Map<String, String>) ?: Collections.<String, String>emptyMap()
        return new AccessUrl(url: url, headers: headers)
    }

    /**
     * Pick the best access method from the list according to
     * {@link #ACCESS_TYPE_PREFERENCE}.  Falls back to the first entry if
     * none of the preferred types are present.
     */
    protected Map chooseAccessMethod(List<Map> accessMethods) {
        for (String preferred : ACCESS_TYPE_PREFERENCE) {
            final match = accessMethods.find { (it.type as String) == preferred }
            if (match)
                return match
        }
        // Fall back to whatever is first
        return accessMethods[0]
    }

    /**
     * HTTP GET a JSON endpoint and return the parsed response as a Map.
     */
    protected Map getJson(String url) throws IOException {
        log.trace "[DRS] GET $url"
        final connection = new URL(url).openConnection() as HttpURLConnection
        try {
            connection.requestMethod = 'GET'
            connection.setRequestProperty('Accept', 'application/json')
            if (bearerToken)
                connection.setRequestProperty('Authorization', "Bearer ${bearerToken}")

            final status = connection.responseCode
            if (status < 200 || status >= 300) {
                final body = connection.errorStream?.text ?: '(no body)'
                throw new IOException("DRS request to '${url}' failed with HTTP ${status}: ${body}")
            }

            return new JsonSlurper().parse(connection.inputStream) as Map
        }
        finally {
            connection.disconnect()
        }
    }

    // -------------------------------------------------------------------------
    // Value type returned by resolve()
    // -------------------------------------------------------------------------

    /**
     * The concrete download URL and any HTTP headers required to access it,
     * as returned by the DRS server.
     */
    static class AccessUrl {
        /** The resolved HTTPS (or other scheme) download URL */
        String url
        /**
         * Optional HTTP headers that must be forwarded when downloading,
         * e.g. {@code Authorization: Bearer <token>} for presigned URLs
         * that also require an auth header.
         */
        Map<String, String> headers = Collections.emptyMap()

        @Override
        String toString() { "AccessUrl(url=$url, headers=$headers)" }
    }
}
