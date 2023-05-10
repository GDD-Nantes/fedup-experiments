package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.iterator.QueryIterUnion;
import org.apache.jena.util.iterator.NullIterator;

import java.util.List;
import java.util.Objects;

/**
 * Unions create an iterator that concatenate operation. We want this
 * iterator to remember the state it was on before pausing so it can resume
 * and execute in the same state.
 **/
public class PreemptQueryIterUnion extends QueryIterUnion {

    SageInput<?> input;
    SageOutput<?> output;

    public PreemptQueryIterUnion(QueryIterator qIter, List<Op> subOps, ExecutionContext context) {
        super(qIter, subOps, context);
        input = getExecContext().getContext().get(SageConstants.input);
        output = getExecContext().getContext().get(SageConstants.output);
    }

    @Override
    protected QueryIterator nextStage(Binding binding) {
        Integer id = getExecContext().getContext().get(SageConstants.cursor);
        id += 1;
        getExecContext().getContext().set(SageConstants.cursor, id);

        // The first time we pause/resume, we want to skip even the creation of
        // iterators. Thus, the first `PreemptQueryIterConcat` is shorter or equal
        // compared to the rest of such iterators that will be created on `nextStage`.
        Integer skipCreatingIterators = 0;
        if (Objects.nonNull(input.getState()) && input.getState().containsKey(id)) {
            skipCreatingIterators = (int) input.getState(id);
        }
        Integer skipToOffset = skipCreatingIterators;

        PreemptQueryIterConcat unionQIter = new PreemptQueryIterConcat(getExecContext(), id);
        for (Op subOp : subOps) {
            if (skipCreatingIterators > 0) {
                skipCreatingIterators -= 1;
                continue;
            }
            subOp = QC.substitute(subOp, binding);
            QueryIterator parent = QueryIterSingleton.create(binding, getExecContext());
            QueryIterator qIter = QC.execute(subOp, parent, getExecContext());
            unionQIter.add(qIter);
        }

        // Set the offset of the first, so it knows the offset when pausing
        unionQIter.skip(skipToOffset);

        return unionQIter;
    }
}
