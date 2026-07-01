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
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemNotFoundException
import java.nio.file.LinkOption
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.ProviderMismatchException
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.spi.FileSystemProvider

/**
 * Read-only JSR-203 {@link FileSystemProvider} for the {@code drs://} scheme.
 *
 * <h3>TES pass-through</h3>
 * When a {@link DrsPath} is used as a task input under the TES executor,
 * {@link DrsPath#toUri()} returns the original {@code drs://} URI, which is
 * placed verbatim into {@code TesInput.url}. Resolution to a concrete download
 * URL therefore never happens on the Nextflow side; the TES backend is
 * responsible for fetching the object.
 *
 * <h3>Local execution</h3>
 * When Nextflow itself needs to read a DRS object (e.g. local executor, or
 * staging to a shared work directory), {@link #newByteChannel} resolves the
 * {@code drs://} URI via {@link DrsClient} and streams the bytes from the
 * returned HTTPS URL.
 *
 * <h3>Authentication</h3>
 * A Bearer token may be configured via {@code drs.accessToken} in
 * {@code nextflow.config}. It is forwarded to every DRS metadata request.
 * The resolved {@link DrsClient.AccessUrl} may itself carry additional headers
 * (e.g. a presigned-URL auth header) that are forwarded to the final download.
 *
 * @author nf-ga4gh contributors
 */
@Slf4j
@CompileStatic
class DrsFileSystemProvider extends FileSystemProvider {

    static final String SCHEME = 'drs'

    private final Map<URI, FileSystem> fileSystemMap = new LinkedHashMap<>(20)

    @Override
    String getScheme() { SCHEME }

    // -------------------------------------------------------------------------
    // FileSystem lifecycle
    // -------------------------------------------------------------------------

    @Override
    FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
        if (uri.scheme?.toLowerCase() != SCHEME)
            throw new IllegalArgumentException("Not a valid DRS URI scheme: ${uri.scheme}")
        final key = baseKey(uri)
        if (fileSystemMap.containsKey(key))
            throw new IllegalStateException("DRS file system already exists for: $key")
        return new DrsFileSystem(this, key)
    }

    @Override
    FileSystem getFileSystem(URI uri) {
        getOrCreateFileSystem(uri, false)
    }

    private FileSystem getOrCreateFileSystem(URI uri, boolean canCreate) {
        final key = baseKey(uri)
        if (!canCreate) {
            final fs = fileSystemMap[key]
            if (!fs) throw new FileSystemNotFoundException("DRS file system not found: $key")
            return fs
        }
        synchronized (fileSystemMap) {
            FileSystem fs = fileSystemMap[key]
            if (!fs) {
                fs = new DrsFileSystem(this, key)
                fileSystemMap[key] = fs
            }
            return fs
        }
    }

    @Override
    Path getPath(URI uri) {
        final fs = (DrsFileSystem) getOrCreateFileSystem(uri, true)
        // The path component of drs://host/object_id is /object_id
        return new DrsPath(fs, uri.path ?: '/')
    }

    /** Normalised base key: {@code drs://host} (lower-cased, no trailing slash) */
    private static URI baseKey(URI uri) {
        new URI(SCHEME, uri.authority?.toLowerCase(), null, null, null)
    }

    // -------------------------------------------------------------------------
    // Core I/O — resolution happens here, lazily, for local execution only
    // -------------------------------------------------------------------------

    @Override
    SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        checkDrsPath(path)
        for (OpenOption opt : options) {
            if (opt == StandardOpenOption.APPEND || opt == StandardOpenOption.WRITE)
                throw new UnsupportedOperationException("'$opt' not allowed on DRS paths")
        }

        final drsUri = path.toUri()
        log.debug "[DRS] Opening byte channel for: $drsUri"

        final accessUrl = newDrsClient().resolve(drsUri)
        log.debug "[DRS] Resolved to: ${accessUrl.url}"

        final conn = new URL(accessUrl.url).openConnection() as HttpURLConnection
        accessUrl.headers.each { k, v -> conn.setRequestProperty(k, v) }
        conn.connect()

        final stream = new BufferedInputStream(conn.inputStream)

        return new SeekableByteChannel() {
            private long _position = 0

            @Override
            int read(ByteBuffer buffer) throws IOException {
                int len = 0
                int data
                while (buffer.hasRemaining() && (data = stream.read()) != -1) {
                    buffer.put((byte) data)
                    len++
                }
                _position += len
                return len ?: -1
            }

            @Override int write(ByteBuffer src) { throw new UnsupportedOperationException('Write not supported') }
            @Override long position() { _position }
            @Override SeekableByteChannel position(long newPos) { throw new UnsupportedOperationException('Seek not supported') }
            // Return a modest default; callers like Files.readAllBytes use this as a buffer-size hint
            @Override long size() { 8192L }
            @Override SeekableByteChannel truncate(long size) { throw new UnsupportedOperationException('Truncate not supported') }
            @Override boolean isOpen() { true }
            @Override void close() { stream.close(); conn.disconnect() }
        }
    }

    @Override
    InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        checkDrsPath(path)
        final drsUri = path.toUri()
        log.debug "[DRS] Opening input stream for: $drsUri"

        final accessUrl = newDrsClient().resolve(drsUri)
        final conn = new URL(accessUrl.url).openConnection() as HttpURLConnection
        accessUrl.headers.each { k, v -> conn.setRequestProperty(k, v) }
        conn.connect()
        return conn.inputStream
    }

    // -------------------------------------------------------------------------
    // Attribute / access checks
    // -------------------------------------------------------------------------

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        checkDrsPath(path)
        for (AccessMode m : modes) {
            if (m == AccessMode.WRITE)
                throw new AccessDeniedException('Write mode not supported by DRS file system')
            if (m == AccessMode.EXECUTE)
                throw new AccessDeniedException('Execute mode not supported by DRS file system')
        }
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        checkDrsPath(path)
        if (type != BasicFileAttributes && type != DrsFileAttributes)
            throw new UnsupportedOperationException("Unsupported attributes type '${type}' for DRS file system")
        final stat = newDrsClient().stat(path.toUri())
        return (A) new DrsFileAttributes(stat)
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        checkDrsPath(path)
        final attrs = readAttributes(path, BasicFileAttributes, options)
        return [
            size           : attrs.size(),
            creationTime   : attrs.creationTime(),
            lastModifiedTime: attrs.lastModifiedTime(),
            lastAccessTime : attrs.lastAccessTime(),
            isRegularFile  : attrs.isRegularFile(),
            isDirectory    : attrs.isDirectory(),
            isSymbolicLink : attrs.isSymbolicLink(),
            isOther        : attrs.isOther(),
            fileKey        : attrs.fileKey(),
        ] as Map<String, Object>
    }

    @Override
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        return null
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException('setAttribute not supported by DRS file system')
    }

    // -------------------------------------------------------------------------
    // Unsupported mutating operations
    // -------------------------------------------------------------------------

    @Override
    OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        throw new UnsupportedOperationException('Write not supported by DRS file system')
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        throw new UnsupportedOperationException('Directory listing not supported by DRS file system')
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        throw new UnsupportedOperationException('createDirectory not supported by DRS file system')
    }

    @Override
    void delete(Path path) throws IOException {
        throw new UnsupportedOperationException('delete not supported by DRS file system')
    }

    @Override
    void copy(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException('copy not supported by DRS file system')
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        throw new UnsupportedOperationException('move not supported by DRS file system')
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        path == path2
    }

    @Override
    boolean isHidden(Path path) throws IOException { false }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException('FileStore not supported by DRS file system')
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void checkDrsPath(Path path) {
        if (path.class != DrsPath)
            throw new ProviderMismatchException()
    }

    /**
     * Create a {@link DrsClient} configured with the optional Bearer token
     * from {@code nextflow.config} ({@code drs.accessToken}).
     *
     * The metadata API scheme defaults to {@code https}; set
     * {@code drs.metadataScheme = 'http'} in {@code nextflow.config} to target
     * a plain-HTTP DRS server (e.g. a local instance during development).
     *
     * Falls back gracefully when no Nextflow session is active (e.g. in tests).
     */
    protected DrsClient newDrsClient() {
        String token = null
        String metadataScheme = 'https'
        try {
            final session = Global.session as Session
            token = session?.config?.navigate('drs.accessToken') as String
            final configuredScheme = session?.config?.navigate('drs.metadataScheme') as String
            if (configuredScheme)
                metadataScheme = configuredScheme
        }
        catch (Exception e) {
            log.trace "[DRS] Could not read session config: ${e.message}"
        }
        return new DrsClient(token, metadataScheme)
    }
}
