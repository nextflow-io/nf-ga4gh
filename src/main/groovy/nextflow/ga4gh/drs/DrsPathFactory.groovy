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

import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileSystemPathFactory
import org.pf4j.Extension

/**
 * Bridges {@code drs://} URI strings to {@link DrsPath} objects for Nextflow.
 *
 * Nextflow resolves custom URI schemes used in {@code file()} / {@code fromPath()}
 * through the {@link FileSystemPathFactory} extension point rather than the raw
 * JVM {@code java.nio} provider SPI. Without this factory, Nextflow reports
 * "Cannot find a file system provider for scheme: drs" even though
 * {@link DrsFileSystemProvider} is registered as an SPI provider.
 *
 * @author nf-ga4gh contributors
 */
@Extension
@Slf4j
@CompileStatic
class DrsPathFactory extends FileSystemPathFactory {

    private static final String PREFIX = "${DrsFileSystemProvider.SCHEME}://"

    private final DrsFileSystemProvider provider = new DrsFileSystemProvider()

    /**
     * Parse a {@code drs://host/object_id} string into a {@link DrsPath}.
     * Returns {@code null} for any non-DRS URI so other factories get a chance.
     */
    @Override
    protected Path parseUri(String uriString) {
        if (!uriString?.startsWith(PREFIX))
            return null
        try {
            return provider.getPath(new URI(uriString))
        }
        catch (Exception e) {
            log.debug "[DRS] Not a parseable DRS URI '${uriString}': ${e.message}"
            return null
        }
    }

    /**
     * Render a {@link DrsPath} back to its {@code drs://} URI string.
     * Returns {@code null} for non-DRS paths.
     */
    @Override
    protected String toUriString(Path path) {
        return (path instanceof DrsPath) ? path.toUri().toString() : null
    }

    /** DRS is a read-only input source; no bash staging helpers are provided. */
    @Override
    protected String getBashLib(Path path) { return null }

    /** DRS is a read-only input source; uploads are not supported. */
    @Override
    protected String getUploadCmd(String source, Path path) { return null }
}
