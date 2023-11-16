package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Objects;


/**
 * Volcano iterator that wraps a backend iterator (the latter initially
 * meant for compiled query execution).
 * Among others, `next()` returns {@link Quad} which
 * contains four {@link Node}; and the `hasNext()` checks if it
 * reached the timeout before saving into the shared
 * {@link SageOutput}.
 */
public class PreemptScanIteratorQuad implements Iterator<Quad> {

    public BackendIterator<NodeId, Serializable> wrapped;
    NodeTable nodeTable;

    SageInput<?>  input;
    SageOutput<?> output;
    Integer id;

    // Cannot pause at first execution of the `hasNext()`.
    boolean first = false;


    
    public PreemptScanIteratorQuad(BackendIterator<NodeId, Serializable> wrapped, NodeTable nodeTable,
                                   SageInput<?> input, SageOutput<?> output, Integer id) {
        this.wrapped = wrapped;
        this.nodeTable = nodeTable;
        this.input = input;
        this.output = output;
        this.id = id;
    }
    
    @Override
    public boolean hasNext() {
        if (!first && (System.currentTimeMillis() > input.getDeadline() || output.size() >= input.getLimit())) {
            Pair toSave = Objects.isNull(this.output.getState()) ?
                    new Pair(id, this.wrapped.current()):
                    new Pair(id, this.wrapped.previous());
            this.output.addState(toSave);

            return false;
        }
        first = false;
        
        return wrapped.hasNext();
    }

    @Override
    public Quad next() {
        wrapped.next();

        Node gx = Objects.isNull(wrapped.getId(SPOC.CONTEXT)) ? Quad.defaultGraphIRI :
                nodeTable.getNodeForNodeId(wrapped.getId(SPOC.GRAPH));
        Node sx = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.SUBJECT));
        Node px = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.PREDICATE));
        Node ox = nodeTable.getNodeForNodeId(wrapped.getId(SPOC.OBJECT));

        return Quad.create(gx, sx, px, ox);
    }

    public void skip(SerializableRecord to) {
        first = true; // skip so first `hasNext` is mandatory
        wrapped.skip(to);
    }
}
