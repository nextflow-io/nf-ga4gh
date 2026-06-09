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
 * Unit tests for {@link DrsPath}.
 *
 * The most important invariant is that {@link DrsPath#toUri()} round-trips
 * the original {@code drs://} URI unchanged, because that value is placed
 * verbatim into {@code TesInput.url}.
 */
class DrsPathTest extends Specification {

    private DrsFileSystemProvider provider
    private DrsFileSystem fs

    def setup() {
        provider = new DrsFileSystemProvider()
        fs = new DrsFileSystem(provider, new URI('drs://drs.example.org'))
    }

    // -------------------------------------------------------------------------
    // toUri — the critical TES pass-through invariant
    // -------------------------------------------------------------------------

    def 'toUri should return the original drs:// URI'() {
        when:
        def path = new DrsPath(fs, '/314159')

        then:
        path.toUri() == new URI('drs://drs.example.org/314159')
        path.toUri().scheme == 'drs'
        path.toUri().host == 'drs.example.org'
    }

    def 'toUri should preserve nested object id paths'() {
        when:
        def path = new DrsPath(fs, '/bucket/subdir/object')

        then:
        path.toUri() == new URI('drs://drs.example.org/bucket/subdir/object')
    }

    // -------------------------------------------------------------------------
    // getFileName / getParent
    // -------------------------------------------------------------------------

    def 'getFileName should return the last path segment'() {
        when:
        def path = new DrsPath(fs, '/a/b/c')

        then:
        path.fileName.toString() == 'c'
    }

    def 'getParent should return the parent path'() {
        when:
        def path = new DrsPath(fs, '/a/b/c')

        then:
        path.parent.toString() == '/a/b'
        path.parent.toUri() == new URI('drs://drs.example.org/a/b')
    }

    def 'getParent of a top-level object should return root'() {
        when:
        def path = new DrsPath(fs, '/314159')

        then:
        path.parent.toString() == '/'
    }

    // -------------------------------------------------------------------------
    // equality
    // -------------------------------------------------------------------------

    def 'equal paths should be equal'() {
        expect:
        new DrsPath(fs, '/obj1') == new DrsPath(fs, '/obj1')
    }

    def 'paths with different object IDs should not be equal'() {
        expect:
        new DrsPath(fs, '/obj1') != new DrsPath(fs, '/obj2')
    }

    def 'paths with different file systems should not be equal'() {
        given:
        def fs2 = new DrsFileSystem(provider, new URI('drs://other.host'))

        expect:
        new DrsPath(fs, '/obj1') != new DrsPath(fs2, '/obj1')
    }

    // -------------------------------------------------------------------------
    // toString
    // -------------------------------------------------------------------------

    def 'toString should return only the object path, not the full URI'() {
        when:
        def path = new DrsPath(fs, '/314159')

        then:
        path.toString() == '/314159'
    }

    // -------------------------------------------------------------------------
    // toFile — must throw
    // -------------------------------------------------------------------------

    def 'toFile should throw UnsupportedOperationException'() {
        when:
        new DrsPath(fs, '/obj').toFile()

        then:
        thrown(UnsupportedOperationException)
    }

    // -------------------------------------------------------------------------
    // compareTo
    // -------------------------------------------------------------------------

    def 'compareTo should order by full URI string'() {
        given:
        def a = new DrsPath(fs, '/aaa')
        def b = new DrsPath(fs, '/zzz')

        expect:
        a.compareTo(b) < 0
        b.compareTo(a) > 0
        a.compareTo(a) == 0
    }

    // -------------------------------------------------------------------------
    // isAbsolute / toAbsolutePath
    // -------------------------------------------------------------------------

    def 'absolute drs path should report isAbsolute true'() {
        expect:
        new DrsPath(fs, '/314159').isAbsolute()
    }

    def 'toAbsolutePath returns self'() {
        given:
        def path = new DrsPath(fs, '/314159')
        expect:
        path.toAbsolutePath().is(path)
    }
}
