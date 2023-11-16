package org.apache.jena.sparql.engine.iterator;

import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.PreemptTupleTable;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;

import java.io.Serializable;
import java.util.Iterator;


/**
 * A volcano iterator factory to ease creation of iterators, one per
 * execution context.
 **/
public class PreemptScanIteratorFactory implements ScanIteratorFactory {
    
    SageInput<?> input;
    SageOutput<?> output;

    final private NodeTable quadNodeTable;
    final private NodeTable tripleNodeTable;
    final protected PreemptTupleTable preemptQuadTupleTable;
    final protected PreemptTupleTable preemptTripleTupleTable;

    ExecutionContext context;

    public PreemptScanIteratorFactory(ExecutionContext context) {
        this.output = context.getContext().get(SageConstants.output);
        this.input  = context.getContext().get(SageConstants.input);
        this.context = context;

        var graph = (DatasetGraphTDB) context.getDataset();
        var nodeQuadTupleTable = graph.getQuadTable().getNodeTupleTable();
        var nodeTripleTupleTable = graph.getTripleTable().getNodeTupleTable();
        quadNodeTable = graph.getQuadTable().getNodeTupleTable().getNodeTable();
        tripleNodeTable = graph.getTripleTable().getNodeTupleTable().getNodeTable();
        preemptTripleTupleTable = new PreemptTupleTable(nodeTripleTupleTable.getTupleTable());
        preemptQuadTupleTable = new PreemptTupleTable(nodeQuadTupleTable.getTupleTable());
    }
    
    public Iterator<Quad> getScan(Tuple<NodeId> pattern, Integer id) {
        BackendIterator<NodeId, Serializable> wrapped = null;
        PreemptScanIteratorQuad volcanoIterator = null;
        if (pattern.len() < 4) {
            wrapped = preemptTripleTupleTable.preemptFind(pattern);
            volcanoIterator = new PreemptScanIteratorQuad(wrapped, tripleNodeTable, input, output, id);
        } else {
            wrapped = preemptQuadTupleTable.preemptFind(pattern);
            volcanoIterator = new PreemptScanIteratorQuad(wrapped, quadNodeTable, input, output, id);
        }

        if (input != null && input.getState() != null && input.getState().containsKey(id)) {
            volcanoIterator.skip((SerializableRecord) input.getState(id));
        }

        return volcanoIterator;
    }

    public Iterator<Tuple<NodeId>> getScan(NodeTupleTable nodeTupleTable, Tuple<NodeId> pattern, Var[] vars, Integer id) {
        BackendIterator<NodeId, Serializable> wrapped = null;
        PreemptScanIteratorTupleId volcanoIterator = null;
        if (pattern.len() < 4) {
            wrapped = preemptTripleTupleTable.preemptFind(pattern);
            volcanoIterator = new PreemptScanIteratorTupleId(wrapped, tripleNodeTable, input, output, id, context);
        } else {
            wrapped = preemptQuadTupleTable.preemptFind(pattern);
            volcanoIterator = new PreemptScanIteratorTupleId(wrapped, quadNodeTable, input, output, id, context);
        }

        // Check if it is a preemptive iterator that should jump directly to its resume state.
        if (input != null && input.getState() != null && input.getState().containsKey(id)) {
            volcanoIterator.skip((SerializableRecord) input.getState(id));
        }

        return volcanoIterator;
    }

    public Iterator<Tuple<NodeId>> getScan(Integer id) {
        PreemptScanIteratorTupleId volcanoIterator = new PreemptScanIteratorTupleId(input, output, id, context);

        // Check if it is a preemptive iterator that should jump directly to its resume state.
        if (input != null && input.getState() != null && input.getState().containsKey(id)) {
            volcanoIterator.skip((SerializableRecord) input.getState(id));
        }

        return volcanoIterator;
    }

}
