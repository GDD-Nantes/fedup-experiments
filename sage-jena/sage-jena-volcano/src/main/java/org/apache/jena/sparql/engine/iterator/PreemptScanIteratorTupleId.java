package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.dboe.trans.bplustree.PreemptJenaIterator;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import java.util.Iterator;
import java.util.Objects;

/**
 * A Volcano Iterator that works on {@link Tuple<NodeId>} instead of
 * {@link  org.apache.jena.sparql.core.Quad}. They are supposedly more efficient.
 */
public class PreemptScanIteratorTupleId implements Iterator<Tuple<NodeId>> {

    public BackendIterator<NodeId, SerializableRecord> wrapped;
    NodeTable nodeTable;

    SageInput<?>  input;
    SageOutput<?> output;
    int id;

    // Cannot pause at first execution of the `hasNext()`.
    boolean first = false;
    ExecutionContext context;


    public PreemptScanIteratorTupleId(BackendIterator<NodeId, SerializableRecord> wrapped, NodeTable nodeTable,
                                      SageInput<?> input, SageOutput<?> output, Integer id, ExecutionContext context) {
        this.wrapped = wrapped;
        this.nodeTable = nodeTable;
        this.input = input;
        this.output = output;
        this.id = id;
        this.context = context;
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
    }

    @Override
    public boolean hasNext() {
        boolean someoneStartedSaving = Objects.nonNull(output.getState()) && !output.getState().isEmpty();
        if (someoneStartedSaving ||
                (!first && (System.currentTimeMillis() >= input.getDeadline() || output.size() >= input.getLimit()))) {
            if (Objects.nonNull(this.output.getState()) && (this.output.getState().containsKey(id))) {
                // no saving since a priority id already saved its state.
                // for instance, in union (bgp 1) (bgp 2), when bgp1 saves and returns false, bgp2 will also
                // call its hasNext(), and try to save, with an identical identifier.
                // we want that every part of the union returns false + only the first saver saves.
                return false;
            }

            // The first of all ids is the one to save its current
            boolean shouldSaveCurrent = Objects.isNull(this.output.getState()) ||
                    this.output.getState().keySet().stream().noneMatch(k -> k > id);

            Pair toSave = new Pair(id, shouldSaveCurrent ? this.wrapped.current() : this.wrapped.previous());
            this.output.addState(toSave);
            return false;
        }
        first = false;

        return wrapped.hasNext();
    }

    @Override
    public Tuple<NodeId> next() {
        context.getContext().set(SageConstants.cursor, id);

        wrapped.next();
        return ((PreemptJenaIterator) wrapped).getCurrentTuple();
    }

    public void skip(SerializableRecord to) {
        first = true; // skip so first `hasNext` is mandatory
        wrapped.skip(to);
    }
}
