package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.iterator.QueryIterOptionalIndex;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Basically the same thing than {@link QueryIterOptionalIndex} but calling the preemptive
 * version of the iterator.
 */
public class PreemptQueryIterOptionalIndex extends QueryIterOptionalIndex {

    Op op; // because in `QueryIterOptionalIndex` the field is private…

    SageInput sageInput;

    public PreemptQueryIterOptionalIndex(QueryIterator input, Op op, ExecutionContext context) {
        super(input, op, context);
        this.op = op;
        sageInput = context.getContext().get(SageConstants.input);
    }

    @Override
    protected QueryIterator nextStage(Binding binding) {
        Integer id = getExecContext().getContext().get(SageConstants.cursor);
        id += 1;
        getExecContext().getContext().set(SageConstants.cursor, id);

        Op op2 = QC.substitute(op, binding);
        QueryIterator thisStep = QueryIterSingleton.create(binding, getExecContext());

        QueryIterator cIter = QC.execute(op2, thisStep, super.getExecContext());
        PreemptQueryIterDefaulting preemptCIter = new PreemptQueryIterDefaulting(cIter, binding, getExecContext(), id);

        if (Objects.nonNull(sageInput.getState()) && sageInput.getState().containsKey(id)) {
            preemptCIter.skip((boolean) sageInput.getState(id));
        }

        return preemptCIter;
    }

}

