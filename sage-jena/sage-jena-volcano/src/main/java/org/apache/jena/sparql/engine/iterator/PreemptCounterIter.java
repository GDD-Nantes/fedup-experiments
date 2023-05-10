package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;


/**
 * An iterator whose sole purpose is to count the number of produced binding
 * in order to inform others that it's time to pause/resume when the limit is
 * reached.
 **/
public class PreemptCounterIter extends QueryIter1 {

    SageOutput<?> output;

    public PreemptCounterIter(QueryIterator cIter,
                              ExecutionContext context) {
        super(cIter, context);
        this.output = context.getContext().get(SageConstants.output);
    }

    /**
     * Two changes: It saves when the limit is reached, and the order
     * of checks is not identical to that of {@link QueryIterSlice}.
     **/
    @Override
    protected boolean hasNextBinding() {
        return getInput().hasNext();
    }

    @Override
    protected Binding moveToNextBinding() {
        output.add(null); // +1 to internal counter
        // if (output != null) {
        //     output.add(null); // +1 to internal counter
        // }
        return getInput().nextBinding();
    }

    @Override
    protected void requestSubCancel() {/* nothing */}

    @Override
    protected void closeSubIterator() {/* nothing */}
}


