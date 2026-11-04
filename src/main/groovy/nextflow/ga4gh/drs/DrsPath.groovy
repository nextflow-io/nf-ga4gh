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

import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

/**
 * JSR-203 {@link Path} implementation for the {@code drs://} scheme.
 *
 * The URI is stored as-is and round-trips faithfully through {@link #toUri()},
 * which is what allows TES backends to receive the original {@code drs://}
 * URI in {@code TesInput.url} without any pre-resolution.
 *
 * Resolution to a concrete download URL only happens lazily inside
 * {@link DrsFileSystemProvider#newByteChannel} (i.e. during local execution).
 *
 * @author nf-ga4gh contributors
 */
@CompileStatic
class DrsPath implements Path {

    static final String SCHEME = 'drs'

    private static final String[] EMPTY = []

    private DrsFileSystem fs

    /** The object ID portion of the URI, stored as a local {@link Path} for manipulation */
    private Path objectPath

    /** Only needed to prevent Kryo serialisation issues */
    protected DrsPath() {}

    DrsPath(DrsFileSystem fs, String objectId) {
        this(fs, objectId, EMPTY)
    }

    DrsPath(DrsFileSystem fs, String objectId, String[] more) {
        this.fs = fs
        this.objectPath = Paths.get(objectId ?: '/', more)
    }

    private DrsPath(DrsFileSystem fs, Path objectPath) {
        this.fs = fs
        this.objectPath = objectPath
    }

    private URI getBaseUri() { fs?.getBaseUri() }

    @Override
    FileSystem getFileSystem() { fs }

    @Override
    boolean isAbsolute() { objectPath.isAbsolute() }

    @Override
    Path getRoot() { new DrsPath(fs, '/') }

    @Override
    Path getFileName() {
        final name = objectPath?.getFileName()?.toString()
        return name ? new DrsPath(null, name) : null
    }

    @Override
    Path getParent() {
        final parent = objectPath?.parent
        if (!parent) return null
        return new DrsPath(fs, parent)
    }

    @Override
    int getNameCount() {
        objectPath.toString() ? objectPath.nameCount : 0
    }

    @Override
    Path getName(int index) { new DrsPath(null, objectPath.getName(index).toString()) }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        new DrsPath(null, objectPath.subpath(beginIndex, endIndex).toString())
    }

    @Override
    Path normalize() { new DrsPath(fs, objectPath.normalize()) }

    @Override
    boolean startsWith(Path other) { startsWith(other.toString()) }

    @Override
    boolean startsWith(String other) { objectPath.startsWith(other) }

    @Override
    boolean endsWith(Path other) { endsWith(other.toString()) }

    @Override
    boolean endsWith(String other) { objectPath.endsWith(other) }

    @Override
    Path resolve(Path other) {
        if (this.class != other.class)
            throw new ProviderMismatchException()
        final that = (DrsPath) other
        if (that.fs && this.fs != that.fs) return other
        if (that.objectPath) return new DrsPath(fs, this.objectPath.resolve(that.objectPath))
        return this
    }

    @Override
    Path resolve(String other) { resolve(new DrsPath(null, other)) }

    @Override
    Path resolveSibling(Path other) {
        if (this.class != other.class)
            throw new ProviderMismatchException()
        final that = (DrsPath) other
        if (that.fs && this.fs != that.fs) return other
        if (that.objectPath) {
            final newPath = this.objectPath.resolveSibling(that.objectPath)
            return newPath.isAbsolute() ? new DrsPath(fs, newPath) : new DrsPath(null, newPath)
        }
        return this
    }

    @Override
    Path resolveSibling(String other) { resolveSibling(new DrsPath(null, other)) }

    @Override
    Path relativize(Path other) {
        new DrsPath(null, objectPath.relativize(((DrsPath) other).objectPath))
    }

    /**
     * Returns the original {@code drs://host/object_id} URI.
     * This is the value that ends up in {@code TesInput.url} — the DRS URI
     * is passed through to the TES backend unchanged.
     */
    @Override
    URI toUri() {
        baseUri ? new URI("${baseUri}${objectPath}") : new URI(objectPath.toString())
    }

    @Override
    Path toAbsolutePath() { this }

    @Override
    Path toRealPath(LinkOption... options) throws IOException { this }

    @Override
    File toFile() { throw new UnsupportedOperationException('DRS paths cannot be converted to local File objects') }

    @Override
    String toString() { objectPath.toString() }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException('WatchService not supported by DRS file system')
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        throw new UnsupportedOperationException('WatchService not supported by DRS file system')
    }

    @Override
    Iterator<Path> iterator() {
        final len = getNameCount()
        new Iterator<Path>() {
            int index
            Path current = len ? getName(index++) : null

            @Override
            boolean hasNext() { current != null }

            @Override
            Path next() {
                final result = current
                current = index < len ? getName(index++) : null
                return result
            }

            @Override
            void remove() { throw new UnsupportedOperationException('Remove not supported') }
        }
    }

    @Override
    int compareTo(Path other) {
        this.toUri().toString() <=> other.toUri().toString()
    }

    @Override
    boolean equals(Object other) {
        if (other?.class != DrsPath) return false
        final that = (DrsPath) other
        this.fs == that.fs && this.objectPath == that.objectPath
    }

    @Override
    int hashCode() {
        Objects.hash(fs, objectPath)
    }
}
