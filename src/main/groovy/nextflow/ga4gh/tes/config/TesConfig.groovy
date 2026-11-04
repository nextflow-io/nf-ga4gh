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

package nextflow.ga4gh.tes.config

import groovy.transform.CompileStatic
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description

/**
 * Configuration options for the TES executor plugin.
 */
@CompileStatic
@ScopeName('tes')
@Description('''
The `tes` scope configures the GA4GH Task Execution Service (TES) executor.
''')
class TesConfig implements ConfigScope {

    @ConfigOption
    @Description('''
    TES endpoint URL.
    ''')
    String endpoint

    @ConfigOption
    @Description('''
    Basic authentication username.
    ''')
    String basicUsername

    @ConfigOption
    @Description('''
    Basic authentication password.
    ''')
    String basicPassword

    @ConfigOption
    @Description('''
    TES API timeout in seconds for connect, read, and write operations.
    ''')
    Integer timeout

    @ConfigOption
    @Description('''
    Task polling interval duration (for example `5s` or `30s`).
    ''')
    String pollInterval

    @ConfigOption
    @Description('''
    API key parameter mode: `query` or `header`.
    ''')
    String apiKeyParamMode

    @ConfigOption
    @Description('''
    API key parameter name.
    ''')
    String apiKeyParamName

    @ConfigOption
    @Description('''
    API key value.
    ''')
    String apiKey

    @ConfigOption
    @Description('''
    OAuth bearer token.
    ''')
    String oauthToken

    @ConfigOption
    @Description('''
    Arbitrary tags added to TES tasks.
    ''')
    Map<String, String> tags
}
