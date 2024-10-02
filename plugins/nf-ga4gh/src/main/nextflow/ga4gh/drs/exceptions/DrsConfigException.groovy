package nextflow.ga4gh.drs.exceptions

import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException
/**
 * Exception thrown for errors while creating a DrsConfig object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@CompileStatic
class DrsConfigException extends AbortOperationException {

    DrsConfigException(String message) {
        super(message)
    }
}