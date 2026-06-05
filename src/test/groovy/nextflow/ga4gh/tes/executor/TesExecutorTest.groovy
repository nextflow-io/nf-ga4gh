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

package nextflow.ga4gh.tes.executor

import nextflow.Session
import nextflow.ga4gh.tes.client.auth.ApiKeyAuth
import nextflow.ga4gh.tes.client.auth.HttpBasicAuth
import nextflow.ga4gh.tes.client.auth.OAuth
import spock.lang.Specification

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class TesExecutorTest extends Specification {

    def 'should get endpoint' () {
        given:
        def config = [
            tes: [endpoint: 'http://foo.com']
        ]
        def session = new Session(config)
        def exec = new TesExecutor(session: session)

        when:
        def result = exec.getEndpoint()
        then:
        result == 'http://foo.com'
    }


     def 'should resolve endpoint from config when env not set'() {
        given:
        def config = [tes: [endpoint: 'http://example.com']]
        def session = new Session(config)
        def exec = new TesExecutor(session: session)

        when:
        def result = exec.getEndpoint()

        then:
        result == System.getenv('NXF_EXECUTOR_TES_ENDPOINT') ?: 'http://example.com'
    }

    def 'should configure basic auth'() {
        given:
        def config = [tes: [basicUsername: 'user1', basicPassword: 'pass1']]
        def exec = new TesExecutor(session: new Session(config))

        when:
        def auths = exec.getAuthentications()

        then:
        auths.size() == 1
        auths['basic'] instanceof HttpBasicAuth
        (auths['basic'] as HttpBasicAuth).username == 'user1'
        (auths['basic'] as HttpBasicAuth).password == 'pass1'
    }

    def 'should configure api key auth in query mode'() {
        given:
        def config = [tes: [apiKeyParamName: 'key', apiKey: 'secret123']]
        def exec = new TesExecutor(session: new Session(config))

        when:
        def auths = exec.getAuthentications()

        then:
        auths.size() == 1
        auths['apikey'] instanceof ApiKeyAuth
        (auths['apikey'] as ApiKeyAuth).apiKey == 'secret123'
        (auths['apikey'] as ApiKeyAuth).location == 'query'
        (auths['apikey'] as ApiKeyAuth).paramName == 'key'
    }

    def 'should configure api key auth in header mode'() {
        given:
        def config = [tes: [apiKeyParamName: 'X-Api-Key', apiKey: 'secret123', apiKeyParamMode: 'header']]
        def exec = new TesExecutor(session: new Session(config))

        when:
        def auths = exec.getAuthentications()

        then:
        auths.size() == 1
        auths['apikey'] instanceof ApiKeyAuth
        (auths['apikey'] as ApiKeyAuth).location == 'header'
    }

    def 'should configure oauth'() {
        given:
        def config = [tes: [oauthToken: 'mytoken']]
        def exec = new TesExecutor(session: new Session(config))

        when:
        def auths = exec.getAuthentications()

        then:
        auths.size() == 1
        auths['oauth'] instanceof OAuth
        (auths['oauth'] as OAuth).accessToken == 'mytoken'
    }

    def 'should return no auths when none configured'() {
        given:
        def exec = new TesExecutor(session: new Session([:]))

        when:
        def auths = exec.getAuthentications()

        then:
        auths.isEmpty()
    }

    def 'should configure tags'() {
        given:
        def config = [tes: [tags: [env: 'prod', team: 'bio']]]
        def exec = new TesExecutor(session: new Session(config))

        when:
        def tags = exec.getTags()

        then:
        tags == [env: 'prod', team: 'bio']
        tags.every { k, v -> v instanceof String }
    }

    def 'should stringify GString tag values'() {
        given:
        def myVar = 'dynamic'
        def config = [tes: [tags: [label: "${myVar}-value"]]]
        def exec = new TesExecutor(session: new Session(config))

        when:
        def tags = exec.getTags()

        then:
        tags['label'] == 'dynamic-value'
        tags['label'] instanceof String
    }

    def 'should return null tags when not configured'() {
        given:
        def exec = new TesExecutor(session: new Session([:]))

        when:
        def tags = exec.getTags()

        then:
        tags == null
    }

    def 'should read poll interval from config'() {
        given:
        def config = [tes: [pollInterval: '30s']]
        def session = new Session(config)

        expect:
        session.config.navigate('tes.pollInterval', '5s') == '30s'
    }

    def 'should use default poll interval when not configured'() {
        given:
        def session = new Session([:])

        expect:
        session.config.navigate('tes.pollInterval', '5s') == '5s'
    }
}
