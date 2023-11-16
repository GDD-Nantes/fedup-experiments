package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.IdentifierAllocator;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.interfaces.PreemptIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Simple extension of slice iterators that are used for implementing LIMIT and OFFSET.
 * We need to record the position of the iterator.
 */
public class PreemptQueryIterSlice extends QueryIterSlice implements PreemptIterator<Long> {

    private static Logger log = LoggerFactory.getLogger(PreemptQueryIterSlice.class);

    Integer id;
    Long previous;

    public PreemptQueryIterSlice (OpSlice op, QueryIterator cIter, long startPosition, long numItems, ExecutionContext context) {
        super(cIter, startPosition, numItems, context);

        Integer current = getExecContext().getContext().get(SageConstants.cursor);
        IdentifierAllocator allocator = new IdentifierAllocator(current);
        op.getSubOp().visit(allocator);
        this.id = allocator.getCurrent() + 1;
        log.debug("Got the identifier : {}.", this.id);

        HashMap<Integer, PreemptIterator> iterators = context.getContext().get(SageConstants.iterators);
        iterators.put(id, this);

        SageInput input = getExecContext().getContext().get(SageConstants.input);
        if (input != null && input.getState() != null && input.getState().containsKey(id)) {
            skip((Long) input.getState(id));
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        previous = count;
        getExecContext().getContext().set(SageConstants.cursor, id);
        return super.moveToNextBinding();
    }

    /* ********************************************************************** */

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public void skip(Long to) {
        this.count = to;
    }

    @Override
    public Long current() {
        return count;
    }

    @Override
    public Long previous() {
        return previous;
    }
}


