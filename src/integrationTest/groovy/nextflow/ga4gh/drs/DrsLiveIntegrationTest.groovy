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

import spock.lang.IgnoreIf
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files

/**
 * End-to-end DRS test against a real, running Syfon server backed by MinIO
 * (see {@code .github/workflows/drs.yml}). Exercises the full path a
 * workflow would take: upload an object via the {@code syfon} CLI, resolve
 * its {@code drs://} URI through {@link DrsFileSystemProvider}, and read
 * the bytes back through a real presigned S3 URL.
 *
 * Skipped unless {@code SYFON_BASE_URL} is set, so it never runs as part of
 * {@code ./gradlew test} — only {@code ./gradlew integrationTest} with the
 * live services from the CI workflow (or a manually started Syfon/MinIO,
 * see the PR description for setup steps).
 */
@IgnoreIf({ !System.getenv('SYFON_BASE_URL') })
class DrsLiveIntegrationTest extends Specification {

    @Shared String syfonBin    = System.getenv('SYFON_BIN')    ?: 'syfon'
    @Shared String baseUrl     = System.getenv('SYFON_BASE_URL')
    @Shared String username    = System.getenv('SYFON_USER')
    @Shared String password    = System.getenv('SYFON_PASS')
    @Shared String org         = System.getenv('SYFON_ORG')
    @Shared String project     = System.getenv('SYFON_PROJECT')

    @Shared String drsHost
    @Shared String drsScheme

    def setupSpec() {
        final matcher = (baseUrl =~ /^(https?):\/\/(.+)$/)
        assert matcher.matches() : "SYFON_BASE_URL must start with http:// or https://, got: $baseUrl"
        drsScheme = matcher.group(1)
        drsHost = matcher.group(2)
    }

    /**
     * Upload a file via the real `syfon` CLI and return the object's DID.
     * {@code --overwrite} makes this safe to rerun against a Syfon instance
     * that already has records from a previous (e.g. retried) test run, since
     * DIDs are derived deterministically from content + project scope.
     */
    private String uploadFile(File file) {
        final cmd = [
            syfonBin, '--server', baseUrl, '--username', username, '--password', password,
            'upload', '--file', file.absolutePath, '--org', org, '--project', project, '--overwrite',
        ]
        final proc = cmd.execute()
        final out = new StringBuilder()
        final err = new StringBuilder()
        proc.consumeProcessOutput(out, err)
        proc.waitForOrKill(30_000)
        if (proc.exitValue() != 0)
            throw new IllegalStateException("syfon upload failed (exit ${proc.exitValue()}): ${err}\n${out}")

        final matcher = (out.toString() =~ /(?m)^successfully uploaded (\S+)/)
        if (!matcher.find())
            throw new IllegalStateException("Could not parse DID from syfon upload output:\n${out}")
        return matcher.group(1)
    }

    private DrsPath drsPathFor(String did) {
        final provider = new DrsFileSystemProvider() {
            @Override
            protected DrsClient newDrsClient() {
                new DrsClient(null, drsScheme, username, password)
            }
        }
        final fs = new DrsFileSystem(provider, new URI("drs://${drsHost}"))
        new DrsPath(fs, "/${did}")
    }

    def 'should upload via syfon and read the object back through a drs:// URI'() {
        given: 'a local file with known content'
        final file = File.createTempFile('nf-ga4gh-drs-itest', '.txt')
        file.text = 'hello from the DRS integration test\n'

        when: 'the file is uploaded to Syfon and resolved via its DRS URI'
        final did = uploadFile(file)
        final content = Files.readString(drsPathFor(did))

        then:
        content == file.text
    }

    def 'should report correct size via readAttributes without downloading'() {
        given:
        final file = File.createTempFile('nf-ga4gh-drs-itest-stat', '.txt')
        file.text = 'x' * 1234

        when:
        final did = uploadFile(file)
        final path = drsPathFor(did)
        final attrs = Files.readAttributes(path, java.nio.file.attribute.BasicFileAttributes)

        then:
        attrs.size() == 1234L
    }

    def 'should surface an IOException for an unknown DID'() {
        when:
        Files.readString(drsPathFor('00000000-0000-0000-0000-000000000000'))

        then:
        thrown(IOException)
    }
}
