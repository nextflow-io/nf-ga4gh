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

import spock.lang.Specification

/**
 * Unit tests for {@link DrsClient}.
 */
class DrsClientTest extends Specification {

    // -------------------------------------------------------------------------
    // chooseAccessMethod
    // -------------------------------------------------------------------------

    def 'should prefer https access method'() {
        given:
        def client = new DrsClient()
        def methods = [
            [type: 'ftp',   access_url: [url: 'ftp://example.org/file']],
            [type: 'https', access_url: [url: 'https://example.org/file']],
            [type: 's3',    access_id: 'abc123'],
        ]

        expect:
        client.chooseAccessMethod(methods).type == 'https'
    }

    def 'should prefer s3 over gs and ftp when https absent'() {
        given:
        def client = new DrsClient()
        def methods = [
            [type: 'gs',  access_id: 'gs-id'],
            [type: 's3',  access_id: 's3-id'],
            [type: 'ftp', access_url: [url: 'ftp://example.org/file']],
        ]

        expect:
        client.chooseAccessMethod(methods).type == 's3'
    }

    def 'should fall back to first method when no preferred type matches'() {
        given:
        def client = new DrsClient()
        def methods = [
            [type: 'az',   access_id: 'az-id'],
            [type: 'htsget', access_id: 'hts-id'],
        ]

        expect:
        client.chooseAccessMethod(methods).type == 'az'
    }

    // -------------------------------------------------------------------------
    // resolve — inline access_url (no second hop needed)
    // -------------------------------------------------------------------------

    def 'should resolve inline access_url without a second hop'() {
        given:
        def client = Spy(DrsClient) {
            getJson('https://drs.example.org/ga4gh/drs/v1/objects/314159') >> [
                access_methods: [
                    [type: 'https', access_url: [url: 'https://storage.example.org/file.bam', headers: ['X-Token': 'tok']]]
                ]
            ]
        }

        when:
        def result = client.resolve(new URI('drs://drs.example.org/314159'))

        then:
        result.url == 'https://storage.example.org/file.bam'
        result.headers == ['X-Token': 'tok']
    }

    def 'should resolve inline access_url with no headers'() {
        given:
        def client = Spy(DrsClient) {
            getJson('https://drs.example.org/ga4gh/drs/v1/objects/abc') >> [
                access_methods: [
                    [type: 'https', access_url: [url: 'https://cdn.example.org/data.vcf']]
                ]
            ]
        }

        when:
        def result = client.resolve(new URI('drs://drs.example.org/abc'))

        then:
        result.url == 'https://cdn.example.org/data.vcf'
        result.headers.isEmpty()
    }

    // -------------------------------------------------------------------------
    // resolve — access_id requiring second hop
    // -------------------------------------------------------------------------

    def 'should resolve via access_id second hop'() {
        given:
        def client = Spy(DrsClient) {
            getJson('https://drs.example.org/ga4gh/drs/v1/objects/314159') >> [
                access_methods: [
                    [type: 'https', access_id: 'my-access-id']
                ]
            ]
            getJson('https://drs.example.org/ga4gh/drs/v1/objects/314159/access/my-access-id') >> [
                url: 'https://presigned.example.org/file.bam?sig=xyz'
            ]
        }

        when:
        def result = client.resolve(new URI('drs://drs.example.org/314159'))

        then:
        result.url == 'https://presigned.example.org/file.bam?sig=xyz'
        result.headers.isEmpty()
    }

    // -------------------------------------------------------------------------
    // resolve — error handling
    // -------------------------------------------------------------------------

    def 'should throw when drs object has no access_methods'() {
        given:
        def client = Spy(DrsClient) {
            getJson(_) >> [access_methods: []]
        }

        when:
        client.resolve(new URI('drs://drs.example.org/empty-obj'))

        then:
        def e = thrown(IOException)
        e.message.contains('no access_methods')
    }

    def 'should throw when access method has neither access_url nor access_id'() {
        given:
        def client = Spy(DrsClient) {
            getJson(_) >> [
                access_methods: [[type: 'https']]
            ]
        }

        when:
        client.resolve(new URI('drs://drs.example.org/bad-obj'))

        then:
        def e = thrown(IOException)
        e.message.contains('neither access_url nor access_id')
    }

    def 'should throw when URI scheme is not drs'() {
        given:
        def client = new DrsClient()

        when:
        client.resolve(new URI('https://example.org/file'))

        then:
        thrown(IllegalArgumentException)
    }

    // -------------------------------------------------------------------------
    // resolve — port handling
    // -------------------------------------------------------------------------

    def 'should include non-default port in metadata URL'() {
        given:
        def client = Spy(DrsClient) {
            getJson('https://drs.example.org:8080/ga4gh/drs/v1/objects/obj1') >> [
                access_methods: [
                    [type: 'https', access_url: [url: 'https://storage.example.org/obj1']]
                ]
            ]
        }

        when:
        def result = client.resolve(new URI('drs://drs.example.org:8080/obj1'))

        then:
        result.url == 'https://storage.example.org/obj1'
    }
}
