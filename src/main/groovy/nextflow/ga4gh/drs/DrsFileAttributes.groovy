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

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime

import groovy.transform.CompileStatic
import nextflow.ga4gh.drs.DrsClient.DrsStat

/**
 * {@link BasicFileAttributes} backed by DRS object metadata (hop 1).
 *
 * A DRS object is always treated as a regular file. Size and last-modified
 * time come from the DRS {@code size} / {@code updated_time} fields; this is
 * what lets Nextflow stage a {@code drs://} input into a task work directory.
 *
 * @author nf-ga4gh contributors
 */
@CompileStatic
class DrsFileAttributes implements BasicFileAttributes {

    private final long size
    private final FileTime modifiedTime

    DrsFileAttributes(DrsStat stat) {
        this.size = stat.size
        this.modifiedTime = FileTime.fromMillis(stat.modifiedMillis)
    }

    @Override
    FileTime lastModifiedTime() { modifiedTime }

    @Override
    FileTime lastAccessTime() { modifiedTime }

    @Override
    FileTime creationTime() { modifiedTime }

    @Override
    boolean isRegularFile() { true }

    @Override
    boolean isDirectory() { false }

    @Override
    boolean isSymbolicLink() { false }

    @Override
    boolean isOther() { false }

    @Override
    long size() { size }

    @Override
    Object fileKey() { null }
}
