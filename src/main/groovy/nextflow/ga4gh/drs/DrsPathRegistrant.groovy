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
import nextflow.util.SerializerRegistrant
import org.pf4j.Extension

/**
 * Registers {@link DrsPathSerializer} as the Kryo serializer for
 * {@link DrsPath} objects.
 *
 * Picked up by Nextflow's plugin system via the {@code extensions.idx} file
 * (generated from the {@code extensionPoints} list in {@code build.gradle}).
 *
 * @author nf-ga4gh contributors
 */
@Extension
@CompileStatic
class DrsPathRegistrant implements SerializerRegistrant {

    @Override
    void register(Map<Class, Object> serializers) {
        serializers.put(DrsPath, DrsPathSerializer)
    }
}
