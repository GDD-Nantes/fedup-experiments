package fr.gdd.sage.configuration;

import fr.gdd.sage.arq.SageConstants;
import org.apache.jena.sparql.util.Context;

import java.util.Objects;

/**
 * A basic Sage configuration that will define the conditions of query
 * execution.
 **/
public class SageServerConfiguration {

    /** The maximum number of results or random walks **/
    long limit   = Long.MAX_VALUE;
    /** The maximum duration before stopping the execution and
        returning the possibly partial results. **/
    long timeout = Long.MAX_VALUE;

    public SageServerConfiguration() { }

    public SageServerConfiguration(Context context) {
        if (Objects.nonNull(context.get(SageConstants.limit)))
            this.limit   = context.getLong(SageConstants.limit, Long.MAX_VALUE);

        if (Objects.nonNull(context.get(SageConstants.timeout)))
            this.timeout = context.getLong(SageConstants.timeout, Long.MAX_VALUE);
    }
    
    public long getLimit() {
        return limit;
    }

    public long getTimeout() {
        return timeout;
    }

    public SageServerConfiguration setLimit(long limit) {
        this.limit = limit;
        return this;
    }

    public SageServerConfiguration setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

}
