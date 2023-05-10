package org.apache.jena.tdb2.solver; // In this package so it can access SolverLibTDB package functions.

import fr.gdd.sage.arq.SageConstants;
import org.apache.jena.sparql.engine.iterator.PreemptScanIteratorFactory;
import fr.gdd.sage.arq.ScanIteratorFactory;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.Abortable;
import org.apache.jena.sparql.engine.iterator.QueryIterAbortable;
import org.apache.jena.sparql.engine.main.solver.SolverLib;
import org.apache.jena.sparql.engine.main.solver.SolverRX4;
import org.apache.jena.tdb2.TDBException;
import org.apache.jena.tdb2.lib.TupleLib;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetable.NodeTable;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.jena.sparql.engine.main.solver.SolverLib.tripleHasEmbTripleWithVars;


/**
 * Utility class dedicated to the pattern matching with Sage over TDB2. The main difference with
 * {@link PatternMatchTDB2} lies in the {@link PreemptScanIteratorFactory} call.
 **/
public class PatternMatchSage {

    static Logger log = LoggerFactory.getLogger(PatternMatchSage.class);

    public static QueryIterator execute(GraphTDB graph, BasicPattern pattern, QueryIterator input, Predicate<Tuple<NodeId>> filter, ExecutionContext execCxt) {
        NodeTupleTable ntt = graph.getNodeTupleTable();
        return matchTriplePattern(pattern, input, execCxt);
    }

    public static QueryIterator execute(DatasetGraphTDB ds, Node graphNode, BasicPattern pattern, QueryIterator input, Predicate<Tuple<NodeId>> filter, ExecutionContext execCxt) {
        NodeTupleTable ntt = ds.chooseNodeTupleTable(graphNode);
        return matchQuadPattern(pattern, graphNode, input, execCxt);
    }



    /**
     * Creates the builder for triple patterns.
     */
    public static QueryIterator matchTriplePattern(BasicPattern pattern, QueryIterator input, ExecutionContext context) {
        return match(pattern, input, context);
    }

    /**
     * Creates the build for quad patterns.
     */
    public static QueryIterator matchQuadPattern(BasicPattern pattern, Node graphNode, QueryIterator input, ExecutionContext context) {
        return match(pattern, input, context, graphNode);
    }

    /**
     * Used by triple and quad pattern to create the builder of scans.
     */
    static QueryIterator match(BasicPattern pattern, QueryIterator input, ExecutionContext context, Node... graphNode) {
        if (graphNode.length > 0) {
            if ( Quad.isUnionGraph(graphNode[0]))
                graphNode[0] = Node.ANY;
            if ( Quad.isDefaultGraph(graphNode[0]))
                graphNode = null;
        }
        boolean anyGraph = graphNode != null && graphNode.length > 0 && (Node.ANY.equals(graphNode[0]));
        var graph = graphNode == null || graphNode.length == 0 ? null : graphNode[0];

        DatasetGraphTDB activeGraph = TDBInternal.getDatasetGraphTDB(context.getDataset());
        NodeTupleTable nodeTupleTable = (graph == null) ?
            activeGraph.getTripleTable().getNodeTupleTable() : 
            activeGraph.getQuadTable().getNodeTupleTable();

        Predicate<Tuple<NodeId>> filter = QC2.getFilter(context.getContext());
        
        var conv = SolverLibTDB.convFromBinding(nodeTupleTable.getNodeTable());
        Iterator<BindingNodeId> chain = Iter.map(input, conv);

        List<Abortable> killList = new ArrayList<>();

        int scanId = context.getContext().get(SageConstants.cursor);
        for (Triple triple: pattern.getList()) {
            // create the function that will be called everytime a
            // scan iterator is created.
            scanId += 1;
            log.debug("Pattern <{}> got assigned the identifier {}.", triple, scanId);

            Tuple<Node> patternTuple = null;
            if ( graph == null )
                // 3-tuples
                patternTuple = TupleFactory.create3(triple.getSubject(), triple.getPredicate(), triple.getObject());
            else
                // 4-tuples.
                patternTuple = TupleFactory.create4(graph, triple.getSubject(), triple.getPredicate(), triple.getObject());

            chain = processAsStarPatternOrNot(chain, graph, triple, nodeTupleTable, patternTuple, anyGraph, filter, context, scanId);

            chain = SolverLib.makeAbortable(chain, killList);
        }
        
        Iterator<Binding> iterBinding = SolverLibTDB.convertToNodes(chain, nodeTupleTable.getNodeTable());
        return new QueryIterAbortable(iterBinding, killList, input, context);
    }

    /**
     * Function from {@link SolverRX} `matchQuadPattern`.
     */
    private static Iterator<BindingNodeId> processAsStarPatternOrNot(Iterator<BindingNodeId> chain, Node graphNode, Triple tPattern,
                                                                     NodeTupleTable nodeTupleTable, Tuple<Node> patternTuple, boolean anyGraph,
                                                                     Predicate<Tuple<NodeId>> filter, ExecutionContext execCxt,
                                                                     Integer id){
        if ( SolverRX.DATAPATH ) {
            if ( ! tripleHasEmbTripleWithVars(tPattern) )
                // No RDF-star <<>> with variables.
                return PreemptStageMatchTuple.access(nodeTupleTable, chain, patternTuple, filter, anyGraph, execCxt, id);
        }

        // RDF-star <<>> with variables.
        // This path should work regardless.

        boolean isTriple = (patternTuple.len() == 3);
        NodeTable nodeTable = nodeTupleTable.getNodeTable();

        Function<BindingNodeId, Iterator<BindingNodeId>> step =
                bnid -> find(bnid, nodeTupleTable, graphNode, tPattern, anyGraph, filter, execCxt, id);
        return Iter.flatMap(chain, step);
    }


    /** This function is called every time an iterator is created but
     * the `bnid` changes and contains the bindings created so far
     * that may be injected in the current iterator.
     *
     * This comes from {@link SolverRX}.
     **/
    private static Iterator<BindingNodeId> find(BindingNodeId bnid, NodeTupleTable nodeTupleTable,
                                                Node xGraphNode, Triple xPattern,
                                                boolean anyGraph,
                                                Predicate<Tuple<NodeId>> filter,
                                                ExecutionContext context,
                                                int id) {
        // (TODO) add filter when on NodeId
        if (Objects.nonNull(filter)) {
            throw new UnsupportedOperationException("Filter not supported yet.");
        }
        
        NodeTable nodeTable = nodeTupleTable.getNodeTable();
        Binding input = bnid.isEmpty() ? BindingFactory.empty() : new BindingTDB(bnid, nodeTable);
        Triple tPattern = Substitute.substitute(xPattern, input);
        Node graphNode = Substitute.substitute(xGraphNode, input);

        Node tGraphNode = anyGraph ? Quad.unionGraph : graphNode ;
        // graphNode is ANY for union graph and null for default graph.
        // Var to ANY, Triple Term to ANY.

        Node g = ( graphNode == null ) ? null : SolverLib.nodeTopLevel(graphNode);
        Node s = SolverLib.nodeTopLevel(tPattern.getSubject());
        Node p = SolverLib.nodeTopLevel(tPattern.getPredicate());
        Node o = SolverLib.nodeTopLevel(tPattern.getObject());
        Tuple<Node> patternTuple = ( g == null )
                ? TupleFactory.create3(s, p, o)
                : TupleFactory.create4(g, s, p, o);
        Tuple<NodeId> patternTupleId = TupleLib.tupleNodeIds(nodeTable, patternTuple);

        // We call our factory instead of creating basic iterators.
        ScanIteratorFactory factory = context.getContext().get(SageConstants.scanFactory);
        Iterator<Quad> scanIterator = factory.getScan(patternTupleId, id);

        Iterator<Binding> matched = Iter.iter(scanIterator)
            .map(dQuad->SolverRX4.matchQuad(input, dQuad, tGraphNode, tPattern))
            .removeNulls();
        return SolverLibTDB.convFromBinding(matched, nodeTable);
    }
}

