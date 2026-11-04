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

import groovy.transform.CompileStatic

import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider

/**
 * Read-only JSR-203 {@link FileSystem} for the {@code drs://} scheme.
 *
 * Each instance is scoped to a single DRS host (i.e. authority component of
 * the URI), mirroring the approach used by {@code XFileSystem} for HTTP/FTP.
 *
 * @author nf-ga4gh contributors
 */
@CompileStatic
class DrsFileSystem extends FileSystem {

    private final DrsFileSystemProvider provider

    /** Base URI, e.g. {@code drs://drs.example.org} */
    private final URI base

    /** Only needed to prevent Kryo serialisation issues */
    protected DrsFileSystem() {}

    DrsFileSystem(DrsFileSystemProvider provider, URI base) {
        this.provider = provider
        this.base = base
    }

    URI getBaseUri() { base }

    @Override
    FileSystemProvider provider() { provider }

    @Override
    void close() throws IOException { /* no-op — stateless */ }

    @Override
    boolean isOpen() { true }

    @Override
    boolean isReadOnly() { true }

    @Override
    String getSeparator() { '/' }

    @Override
    Iterable<Path> getRootDirectories() { null }

    @Override
    Iterable<FileStore> getFileStores() { null }

    @Override
    Set<String> supportedFileAttributeViews() { null }

    @Override
    Path getPath(String first, String... more) {
        new DrsPath(this, first, more)
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) { null }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException('UserPrincipalLookupService not supported by DRS file system')
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException('WatchService not supported by DRS file system')
    }

    @Override
    boolean equals(Object other) {
        if (this.class != other?.class) return false
        final that = (DrsFileSystem) other
        this.provider == that.provider && this.base == that.base
    }

    @Override
    int hashCode() {
        Objects.hash(provider, base)
    }
}
