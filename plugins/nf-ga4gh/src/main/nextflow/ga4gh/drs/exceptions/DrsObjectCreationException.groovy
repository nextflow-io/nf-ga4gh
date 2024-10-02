package nextflow.ga4gh.drs.exceptions

import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException
/**
 * Exception thrown for errors while creating a DRS object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@CompileStatic
class DrsObjectCreationException extends AbortOperationException {

    DrsObjectCreationException(String message) {
        super(message)
    }
}