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

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.core.WireMockConfiguration
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files

import static com.github.tomakehurst.wiremock.client.WireMock.*

/**
 * Integration-style tests for {@link DrsFileSystemProvider} using a real
 * embedded HTTP server (WireMock) to stub DRS metadata and download endpoints.
 *
 * These tests exercise the full path from {@code drs://} URI → DRS metadata
 * resolution → file download, without mocking any of our own code.
 *
 * Two resolution paths are covered:
 * <ol>
 *   <li>Inline {@code access_url}: the DRS object carries the download URL
 *       directly in the first response — no second HTTP hop.</li>
 *   <li>{@code access_id} second hop: the first response carries only an
 *       {@code access_id}; a second request to
 *       {@code /ga4gh/drs/v1/objects/{id}/access/{access_id}}
 *       returns the final download URL.</li>
 * </ol>
 */
class DrsFileSystemProviderTest extends Specification {

    @Shared
    WireMockServer wireMock

    // WireMock binds to a random free port so tests don't collide with other services
    @Shared
    int port

    def setupSpec() {
        wireMock = new WireMockServer(WireMockConfiguration.options().dynamicPort())
        wireMock.start()
        port = wireMock.port()
    }

    def cleanupSpec() {
        wireMock.stop()
    }

    def cleanup() {
        // Reset stubs between tests so they don't interfere with each other
        wireMock.resetAll()
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Read a DrsPath to a String via Files.readString (exercises newByteChannel). */
    private String readDrsPath(DrsPath path) {
        Files.readString(path)
    }

    /** Build a DrsPath rooted at the WireMock server. */
    private DrsPath drsPath(String objectId) {
        def provider = new DrsFileSystemProvider() {
            // Use plain HTTP so WireMock (non-TLS) can serve the metadata requests
            @Override
            protected DrsClient newDrsClient() { new DrsClient(null, 'http') }
        }
        def fs = new DrsFileSystem(provider, new URI("drs://localhost:${port}"))
        new DrsPath(fs, "/${objectId}")
    }

    // -------------------------------------------------------------------------
    // Path 1: inline access_url — single HTTP hop
    // -------------------------------------------------------------------------

    def 'should read file via inline access_url (single hop)'() {
        given: 'DRS object endpoint returns an inline https access_url'
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/obj-inline'))
            .willReturn(okJson("""{
                "id": "obj-inline",
                "self_uri": "drs://localhost:${port}/obj-inline",
                "access_methods": [{
                    "type": "https",
                    "access_url": { "url": "http://localhost:${port}/download/obj-inline" }
                }]
            }""")))

        and: 'the download endpoint serves the file content'
        wireMock.stubFor(get(urlEqualTo('/download/obj-inline'))
            .willReturn(ok('hello from drs')))

        when:
        def content = readDrsPath(drsPath('obj-inline'))

        then:
        content == 'hello from drs'

        and: 'exactly one DRS metadata request was made'
        wireMock.verify(1, getRequestedFor(urlEqualTo('/ga4gh/drs/v1/objects/obj-inline')))
        wireMock.verify(1, getRequestedFor(urlEqualTo('/download/obj-inline')))
    }

    def 'should prefer https access_url over s3 when both are present'() {
        given:
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/obj-multi'))
            .willReturn(okJson("""{
                "id": "obj-multi",
                "access_methods": [
                    { "type": "s3",    "access_id": "s3-id" },
                    { "type": "https", "access_url": { "url": "http://localhost:${port}/download/preferred" } }
                ]
            }""")))
        wireMock.stubFor(get(urlEqualTo('/download/preferred'))
            .willReturn(ok('preferred content')))

        when:
        def content = readDrsPath(drsPath('obj-multi'))

        then:
        content == 'preferred content'

        and: 'the /access/ endpoint was never called'
        wireMock.verify(0, getRequestedFor(urlMatching('/ga4gh/drs/v1/objects/obj-multi/access/.*')))
    }

    // -------------------------------------------------------------------------
    // Path 2: access_id second hop — two HTTP hops
    // -------------------------------------------------------------------------

    def 'should read file via access_id second hop'() {
        given: 'DRS object endpoint returns only an access_id'
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/obj-twohop'))
            .willReturn(okJson("""{
                "id": "obj-twohop",
                "access_methods": [{
                    "type": "https",
                    "access_id": "token-abc123"
                }]
            }""")))

        and: 'the access endpoint resolves the access_id to a download URL'
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/obj-twohop/access/token-abc123'))
            .willReturn(okJson("""{ "url": "http://localhost:${port}/download/obj-twohop" }""")))

        and: 'the download endpoint serves the file content'
        wireMock.stubFor(get(urlEqualTo('/download/obj-twohop'))
            .willReturn(ok('two hop content')))

        when:
        def content = readDrsPath(drsPath('obj-twohop'))

        then:
        content == 'two hop content'

        and: 'both DRS hops were made exactly once'
        wireMock.verify(1, getRequestedFor(urlEqualTo('/ga4gh/drs/v1/objects/obj-twohop')))
        wireMock.verify(1, getRequestedFor(urlEqualTo('/ga4gh/drs/v1/objects/obj-twohop/access/token-abc123')))
    }

    // -------------------------------------------------------------------------
    // Auth header forwarding
    // -------------------------------------------------------------------------

    def 'should forward Authorization header from resolved AccessUrl to download'() {
        given: 'DRS returns an access_url with an Authorization header'
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/obj-auth'))
            .willReturn(okJson("""{
                "id": "obj-auth",
                "access_methods": [{
                    "type": "https",
                    "access_url": {
                        "url": "http://localhost:${port}/download/obj-auth",
                        "headers": { "Authorization": "Bearer secret-token" }
                    }
                }]
            }""")))
        wireMock.stubFor(get(urlEqualTo('/download/obj-auth'))
            .withHeader('Authorization', equalTo('Bearer secret-token'))
            .willReturn(ok('auth-gated content')))

        when:
        def content = readDrsPath(drsPath('obj-auth'))

        then:
        content == 'auth-gated content'
    }

    def 'should send bearer token to DRS metadata endpoint when configured'() {
        given: 'a provider configured with a bearer token'
        def provider = new DrsFileSystemProvider() {
            @Override
            protected DrsClient newDrsClient() { new DrsClient('my-drs-token', 'http') }
        }
        def fs = new DrsFileSystem(provider, new URI("drs://localhost:${port}"))
        def path = new DrsPath(fs, '/obj-bearer')

        and:
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/obj-bearer'))
            .withHeader('Authorization', equalTo('Bearer my-drs-token'))
            .willReturn(okJson("""{
                "id": "obj-bearer",
                "access_methods": [{
                    "type": "https",
                    "access_url": { "url": "http://localhost:${port}/download/obj-bearer" }
                }]
            }""")))
        wireMock.stubFor(get(urlEqualTo('/download/obj-bearer'))
            .willReturn(ok('bearer content')))

        when:
        def content = Files.readString(path)

        then:
        content == 'bearer content'

        and: 'the Authorization header was sent to the DRS metadata endpoint'
        wireMock.verify(1, getRequestedFor(urlEqualTo('/ga4gh/drs/v1/objects/obj-bearer'))
            .withHeader('Authorization', equalTo('Bearer my-drs-token')))
    }

    // -------------------------------------------------------------------------
    // TES pass-through invariant
    // -------------------------------------------------------------------------

    def 'toUri should return the original drs:// URI for TES pass-through'() {
        // This is the core invariant that lets TesTaskHandler.inItem() pass
        // drs:// URIs directly to TesInput.url without any pre-resolution.
        when:
        def path = drsPath('314159')

        then:
        path.toUri() == new URI("drs://localhost:${port}/314159")
        path.toUri().scheme == 'drs'

        and: 'no HTTP requests were made — resolution is lazy'
        wireMock.verify(0, getRequestedFor(anyUrl()))
    }

    // -------------------------------------------------------------------------
    // Error handling
    // -------------------------------------------------------------------------

    def 'should throw IOException when DRS server returns 404'() {
        given:
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/missing'))
            .willReturn(notFound().withBody('{"msg": "object not found"}')))

        when:
        readDrsPath(drsPath('missing'))

        then:
        def e = thrown(IOException)
        e.message.contains('404')
    }

    def 'should throw IOException when DRS server returns 500'() {
        given:
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/broken'))
            .willReturn(serverError().withBody('internal error')))

        when:
        readDrsPath(drsPath('broken'))

        then:
        def e = thrown(IOException)
        e.message.contains('500')
    }

    def 'should throw IOException when access_methods list is empty'() {
        given:
        wireMock.stubFor(get(urlEqualTo('/ga4gh/drs/v1/objects/no-methods'))
            .willReturn(okJson('{"id": "no-methods", "access_methods": []}')))

        when:
        readDrsPath(drsPath('no-methods'))

        then:
        def e = thrown(IOException)
        e.message.contains('no access_methods')
    }

    def 'should throw UnsupportedOperationException on write attempt'() {
        when:
        def provider = new DrsFileSystemProvider()
        def fs = new DrsFileSystem(provider, new URI("drs://localhost:${port}"))
        provider.newOutputStream(new DrsPath(fs, '/obj'))

        then:
        thrown(UnsupportedOperationException)
    }
}
