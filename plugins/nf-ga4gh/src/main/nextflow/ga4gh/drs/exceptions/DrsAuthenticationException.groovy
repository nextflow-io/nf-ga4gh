package nextflow.ga4gh.drs.exceptions

import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException
/**
 * Exception thrown for errors while creating a DrsConfig object
 *
 * @author Nicolas Vannieuwkerke <nicolas.vannieuwkerke@ugent.be>
 */
@CompileStatic
class DrsAuthenticationException extends AbortOperationException {

    DrsAuthenticationException(String message) {
        super("Something went wrong during the DRS authentication: " + message)
    }
}