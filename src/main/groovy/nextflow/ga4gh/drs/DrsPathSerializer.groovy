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

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.file.FileHelper

/**
 * Kryo serializer for {@link DrsPath}.
 *
 * Serialises as the plain URI string ({@code drs://host/object_id}) and
 * reconstructs via {@link FileHelper#asPath(URI)}, which routes back through
 * {@link DrsFileSystemProvider#getPath(URI)}.
 *
 * This mirrors the approach used by {@code XPathSerializer} for HTTP/FTP paths.
 *
 * @author nf-ga4gh contributors
 */
@Slf4j
@CompileStatic
class DrsPathSerializer extends Serializer<DrsPath> {

    @Override
    void write(Kryo kryo, Output output, DrsPath target) {
        final uri = target.toUri().toString()
        log.trace "[DRS] Serialising path > uri=$uri"
        output.writeString(uri)
    }

    @Override
    DrsPath read(Kryo kryo, Input input, Class<DrsPath> type) {
        final uri = input.readString()
        log.trace "[DRS] Deserialising path > uri=$uri"
        (DrsPath) FileHelper.asPath(new URI(uri))
    }
}
