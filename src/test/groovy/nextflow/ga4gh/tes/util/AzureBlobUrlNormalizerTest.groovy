/*
 * Copyright (c) Nextflow Contributors
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

package nextflow.ga4gh.tes.util

import spock.lang.Specification

class AzureBlobUrlNormalizerTest extends Specification {

    def 'should normalize path-style url to canonical az format with storage account'() {
        expect:
        AzureBlobUrlNormalizer.normalizeToAzurePath('/mystorage/inputs/work/file.txt') == 'az://mystorage/inputs/work/file.txt'
    }

    def 'should normalize https blob url to canonical az format with storage account'() {
        expect:
        AzureBlobUrlNormalizer.normalizeToAzurePath('https://mystorage.blob.core.windows.net/inputs/work/file.txt') ==
            'az://mystorage/inputs/work/file.txt'
    }

    def 'should preserve canonical az format and trim trailing slash'() {
        expect:
        AzureBlobUrlNormalizer.normalizeToAzurePath('az://mystorage/inputs/work/') == 'az://mystorage/inputs/work'
    }

    def 'should convert canonical az format to https blob url'() {
        expect:
        AzureBlobUrlNormalizer.toHttpsBlobUrl('mystorage', 'az://mystorage/inputs/work/file.txt') ==
            'https://mystorage.blob.core.windows.net/inputs/work/file.txt'
    }

    def 'should convert legacy az format to https blob url using configured account'() {
        expect:
        AzureBlobUrlNormalizer.toHttpsBlobUrl('mystorage', 'az://inputs/work/file.txt') ==
            'https://mystorage.blob.core.windows.net/inputs/work/file.txt'
    }
}
