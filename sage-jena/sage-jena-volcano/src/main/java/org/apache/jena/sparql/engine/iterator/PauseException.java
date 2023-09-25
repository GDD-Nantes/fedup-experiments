package org.apache.jena.sparql.engine.iterator;

import java.io.Serializable;
import java.util.Map;

/**
 * Pause Exception that interrupt the iterator pipeline execution. We don't want any stacktrace
 * not message nor nothing as it may slow down the overall execution:
 * <a href="https://www.baeldung.com/java-exceptions-performance">...</a>
 */
public class PauseException extends RuntimeException {

    public PauseException() {
        super("Pause", null, false, false);
    }

}
