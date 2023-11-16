package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.IdentifierLinker;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.PreemptIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * A Volcano Iterator that works on {@link Tuple<NodeId>} instead of
 * {@link  org.apache.jena.sparql.core.Quad}. They are supposedly more efficient.
 */
public class PreemptScanIteratorTupleId implements Iterator<Tuple<NodeId>>, PreemptIterator<SerializableRecord> {

    private static Logger log = LoggerFactory.getLogger(PreemptScanIteratorTupleId.class);

    public BackendIterator<NodeId, Serializable> wrapped;
    NodeTable nodeTable;

    SageInput<?>  input;
    SageOutput<?> output;
    int id;

    // Cannot pause at first execution of the `hasNext()`.
    boolean first = false;
    ExecutionContext context;


    public PreemptScanIteratorTupleId(BackendIterator<NodeId, Serializable> wrapped, NodeTable nodeTable,
                                      SageInput<?> input, SageOutput<?> output, Integer id, ExecutionContext context) {
        this.wrapped = wrapped;
        this.nodeTable = nodeTable;
        this.input = input;
        this.output = output;
        this.id = id;
        this.context = context;

        HashMap<Integer, PreemptIterator> iterators = context.getContext().get(SageConstants.iterators);
        iterators.put(id, this);
    }

    /**
     * Empty iterator. Still have arguments in case it needs to save
     */
    public PreemptScanIteratorTupleId(SageInput<?> input, SageOutput<?> output, Integer id, ExecutionContext context) {
        wrapped = new PreemptJenaIterator();
        this.context = context;
        this.input = input;
        this.output = output;
        this.id = id;

        HashMap<Integer, PreemptIterator> iterators = context.getContext().get(SageConstants.iterators);
        iterators.put(id, this);
    }

    @Override
    public boolean hasNext() {
        boolean result = wrapped.hasNext();

        if (result) {
            if (!first &&
                    System.currentTimeMillis() >= input.getDeadline() || output.size() >= input.getLimit()) {
                // saving
                HashMap<Integer, PreemptIterator> iterators = context.getContext().get(SageConstants.iterators);
                IdentifierLinker identifiers = context.getContext().get(SageConstants.identifiers);
                Set<Integer> parents = identifiers.getParents(getId());
                log.debug("@{}", id);
                for (Integer parent : parents) {
                    if (!iterators.containsKey(parent)) {
                        // in OPT, the iterator may not exist (because it fails beforehand) while being
                        // a parent of downstream iterator. When it does not exist, we do not save anything
                        // since previous iterator will fail on resume as well.
                        log.debug("SKIP {}", parent);
                        continue;
                    }
                    if (identifiers.inRightSideOf(parent, id)) {
                        log.debug("SAVE CURRENT {}", parent);
                        Pair toSave = new Pair(parent, iterators.get(parent).current());
                        this.output.addState(toSave);
                    } else {
                        log.debug("SAVE PREVIOUS {}", parent);
                        Pair toSave = new Pair(parent, iterators.get(parent).previous());
                        this.output.addState(toSave);
                    }
                }
                this.output.addState(new Pair(getId(), current()));
                // execution stops immediately, caught by {@link PreemptRootIter}
                throw new PauseException();
            }
            return true; // always true
        }

        if (!result) {
            // we unregister the iterator to make sure it's not saved
            HashMap<Integer, PreemptIterator> iterators = context.getContext().get(SageConstants.iterators);
            iterators.remove(id);
        }

        // when false, there is no chance that we save at this point
        return false;
    }

    @Override
    public Tuple<NodeId> next() {
        first = false;

        context.getContext().set(SageConstants.cursor, id);
        wrapped.next();
        return ((PreemptJenaIterator) wrapped).getCurrentTuple();
    }

    /* ******************************************************************************* */

    @Override
    public Integer getId() {
        return this.id;
    }

    @Override
    public void skip(SerializableRecord to) {
        first = true; // skip so first `hasNext` is mandatory
        wrapped.skip(to);
    }

    @Override
    public SerializableRecord current() {
        return (SerializableRecord) wrapped.current();
    }

    @Override
    public SerializableRecord previous() {
        return (SerializableRecord) wrapped.previous();
    }
}
