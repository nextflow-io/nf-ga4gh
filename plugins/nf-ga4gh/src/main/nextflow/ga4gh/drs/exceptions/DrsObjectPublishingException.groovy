package nextflow.ga4gh.drs.exceptions

import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException
/**
 * Exception thrown for errors when publishing a DRS object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@CompileStatic
class DrsObjectPublishingException extends AbortOperationException {

    DrsObjectPublishingException(String message) {
        super(message)
    }
}