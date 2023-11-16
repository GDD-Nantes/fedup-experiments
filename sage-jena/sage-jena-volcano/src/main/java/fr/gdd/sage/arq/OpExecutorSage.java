package fr.gdd.sage.arq;

import fr.gdd.sage.configuration.SageInputBuilder;
import fr.gdd.sage.configuration.SageServerConfiguration;
import fr.gdd.sage.interfaces.PreemptIterator;
import org.apache.jena.sparql.engine.iterator.PreemptQueryIterUnion;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.algebra.optimize.TransformFilterPlacement;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.*;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.PatternMatchSage;
import org.apache.jena.tdb2.solver.PatternMatchTDB2;
import org.apache.jena.tdb2.solver.QC2;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;


/**
 * Some operators need rewriting to enable pausing/resuming their
 * operation. This is mostly a copy/pasta of {@link OpExecutorTDB2}, for we want
 * the same behavior of this executor but the use of our own preemptive iterators.
 * <br />
 * It is worth noting that including preemptive iterators in the standard workflow of
 * TDB2 would be easier if modifying the source code was allowed. Indeed, it would be
 * enough to have a ScanIteratorFactory that instantiates the proper scan iterator depending
 * on the execution context.
 * This modification would take place in two instances only : {@link org.apache.jena.tdb2.solver.SolverRX}
 * Line 116; and {@link org.apache.jena.tdb2.solver.StageMatchTuple} Line 59.
 **/
public class OpExecutorSage extends OpExecutorTDB2 {
    private static Logger log = LoggerFactory.getLogger(OpExecutorSage.class);

    /**
     * Factory to be registered in Jena ARQ. It creates an OpExecutor for
     * Sage in charge of operations customized for pausing/resuming
     * queries.
     */
    public static class OpExecutorSageFactory implements OpExecutorFactory {
        SageServerConfiguration configuration;

        public OpExecutorSageFactory(Context context) {
            configuration = new SageServerConfiguration(context);
        }

        @Override
        public OpExecutor create(ExecutionContext context) {
            return new OpExecutorSage(context, configuration);
        }
    }


    public OpExecutorSage(ExecutionContext context, SageServerConfiguration configuration) {
        super(context);

        // QC.exec creates a new `OpExecutor` and we want to keep our old values, so `SageInput`
        // is created only on the first call.
        if (execCxt.getContext().isUndef(SageConstants.input)) { // <=> setIfUndef
            long limit = execCxt.getContext().getLong(SageConstants.limit, Long.MAX_VALUE);
            long timeout = execCxt.getContext().getLong(SageConstants.timeout, Long.MAX_VALUE);
            Map<Integer, Serializable> state = execCxt.getContext().get(SageConstants.state);
            SageInput<?> input = new SageInputBuilder()
                    .globalConfig(configuration)
                    .localInput(new SageInput<>().setLimit(limit).setTimeout(timeout).setState(state))
                    .build();
            execCxt.getContext().set(SageConstants.input, input);
        }


        // It may have been created by {@link SageQueryEngine} when {@link OpExecutorSage} is not standalone
        execCxt.getContext().setIfUndef(SageConstants.output, new SageOutput<>());
        execCxt.getContext().setIfUndef(SageConstants.scanFactory, new PreemptScanIteratorFactory(execCxt));
        execCxt.getContext().setIfUndef(SageConstants.cursor, 0); // Starting identifier of preemptive iterators
        execCxt.getContext().setIfUndef(SageConstants.iterators, new HashMap<Integer, PreemptIterator>());
    }

    @Override
    protected QueryIterator exec(Op op, QueryIterator input) {
        log.debug(op.getName());
        IdentifierLinker.create(execCxt, op);
        return super.exec(op, input);
    }

    @Override
    protected QueryIterator execute(OpBGP opBGP, QueryIterator input) {
        if (execCxt.getContext().isFalse(ARQ.optimization)) { // force order
            return PatternMatchSage.matchTriplePattern(opBGP.getPattern(), input, execCxt);
        } else { // order of TDB2
            GraphTDB graph = (GraphTDB)this.execCxt.getActiveGraph();
            return executeBGP(graph, opBGP, input, (ExprList)null, this.execCxt);
        }
    }
    
    @Override
    protected QueryIterator execute(OpTriple opTriple, QueryIterator input) {
        return PatternMatchSage.matchTriplePattern(opTriple.asBGP().getPattern(), input, execCxt);
    }
    
    @Override
    protected QueryIterator execute(OpQuadPattern quadPattern, QueryIterator input) {
        if (execCxt.getContext().isFalse(ARQ.optimization)) { // force order
            return PatternMatchSage.matchQuadPattern(quadPattern.getBasicPattern(), quadPattern.getGraphNode(), input, execCxt);
        } else { // order of TDB2
            DatasetGraphTDB ds = (DatasetGraphTDB)this.execCxt.getDataset();
            BasicPattern bgp = quadPattern.getBasicPattern();
            Node gn = quadPattern.getGraphNode();
            return optimizeExecuteQuads(ds, input, gn, bgp, (ExprList)null, this.execCxt);
        }
    }

    @Override
    protected QueryIterator execute(OpQuad opQuad, QueryIterator input) {
        return PatternMatchSage.matchQuadPattern(opQuad.asQuadPattern().getBasicPattern(), opQuad.getQuad().getGraph(), input, execCxt);
    }

    @Override
    public QueryIterator execute(OpUnion opUnion, QueryIterator input) {
        // Comes from {@link OpExecutorTDB2}
        return new PreemptQueryIterUnion(input, flattenUnion(opUnion), execCxt);
    }

    @Override
    protected QueryIterator execute(OpJoin opJoin, QueryIterator input) {
        // Using Sage, we are bound to `NestedLoopJoin`. TDB2's default is hash join.
        return new PreemptQueryIterNestedLoopJoin(opJoin, input, execCxt);
    }

    @Override
    protected QueryIterator execute(OpConditional opCondition, QueryIterator input) {
        // Comes from {@link OpExecutor}:
        QueryIterator left = exec(opCondition.getLeft(), input);
        return new PreemptQueryIterOptionalIndex(opCondition, left, opCondition.getRight(), execCxt);
    }

    @Override
    protected QueryIterator execute(OpLeftJoin opLeftJoin, QueryIterator input) {
        QueryIterator left = exec(opLeftJoin.getLeft(), input);
        return new PreemptQueryIterOptionalIndex(opLeftJoin, left, opLeftJoin.getRight(), execCxt);
    }

    @Override
    protected QueryIterator execute(OpSlice opSlice, QueryIterator input) {
        QueryIterator qIter = exec(opSlice.getSubOp(), input);
        qIter = new PreemptQueryIterSlice(opSlice, qIter, opSlice.getStart(), opSlice.getLength(), execCxt);
        return qIter;
    }

    /**
     * Copy/Pasta from {@link OpExecutorTDB2} again. We want it to use our
     * own `executeBGP` and `optimizeExecuteQuads`.
     */
    @Override
    protected QueryIterator execute(OpFilter opFilter, QueryIterator input) {
        // If the filter does not apply to the input??
        // Where does ARQ catch this?

        if ( OpBGP.isBGP(opFilter.getSubOp()) ) { // (filter (bgp ...))
            // Still may be a TDB graph in a non-TDB dataset (e.g. a named model)
            GraphTDB graph = (GraphTDB)execCxt.getActiveGraph();
            OpBGP opBGP = (OpBGP)opFilter.getSubOp();
            return executeBGP(graph, opBGP, input, opFilter.getExprs(), execCxt);
        }

        if ( opFilter.getSubOp() instanceof OpQuadPattern ) { // (filter (quadpattern ...))
            OpQuadPattern quadPattern = (OpQuadPattern)opFilter.getSubOp();
            DatasetGraphTDB ds = (DatasetGraphTDB)execCxt.getDataset();
            return optimizeExecuteQuads(ds, input,
                    quadPattern.getGraphNode(), quadPattern.getBasicPattern(),
                    opFilter.getExprs(), execCxt);
        }

        // (filter (anything else))
        return super.execute(opFilter, input);
    }


    /* **************************************************************************************************************/

    /**
     * Again copy/past of this specific section of {@link OpExecutorTDB2} where the order of operations
     * could be modified.
     **/

    private static QueryIterator executeBGP(GraphTDB graph, OpBGP opBGP, QueryIterator input, ExprList exprs, ExecutionContext execCxt) {
        DatasetGraphTDB dsgtdb = graph.getDSG();
        return !isDefaultGraphStorage(graph.getGraphName()) ? optimizeExecuteQuads(dsgtdb, input, graph.getGraphName(), opBGP.getPattern(), exprs, execCxt) : optimizeExecuteTriples(dsgtdb, input, opBGP.getPattern(), exprs, execCxt);
    }

    private static QueryIterator optimizeExecuteTriples(DatasetGraphTDB dsgtdb, QueryIterator input, BasicPattern pattern, ExprList exprs, ExecutionContext execCxt) {
        if (!((QueryIterator)input).hasNext()) {
            return (QueryIterator)input;
        } else {
            if (pattern.size() >= 2) {
                ReorderTransformation transform = dsgtdb.getReorderTransform();
                if (transform != null) {
                    QueryIterPeek peek = QueryIterPeek.create((QueryIterator)input, execCxt);
                    input = peek;
                    pattern = reorder(pattern, peek, transform);
                }
            }

            if (exprs == null) {
                Explain.explain("Execute", pattern, execCxt.getContext());
                Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
                return PatternMatchSage.execute(dsgtdb, Quad.defaultGraphNodeGenerated, pattern, (QueryIterator)input, filter, execCxt);
            } else {
                Op op = TransformFilterPlacement.transform(exprs, pattern);
                return plainExecute(op, (QueryIterator)input, execCxt);
            }
        }
    }

    private static QueryIterator optimizeExecuteQuads(DatasetGraphTDB dsgtdb, QueryIterator input, Node gn, BasicPattern bgp, ExprList exprs, ExecutionContext execCxt) {
        if (!((QueryIterator)input).hasNext()) {
            return (QueryIterator)input;
        } else {
            gn = decideGraphNode(gn, execCxt);
            if (gn == null) {
                return optimizeExecuteTriples(dsgtdb, (QueryIterator)input, bgp, exprs, execCxt);
            } else {
                if (bgp.size() >= 2) {
                    ReorderTransformation transform = dsgtdb.getReorderTransform();
                    if (transform != null) {
                        QueryIterPeek peek = QueryIterPeek.create((QueryIterator)input, execCxt);
                        input = peek;
                        bgp = reorder(bgp, peek, transform);
                    }
                }

                if (exprs == null) {
                    Explain.explain("Execute", bgp, execCxt.getContext());
                    Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
                    return PatternMatchSage.execute(dsgtdb, gn, bgp, (QueryIterator)input, filter, execCxt);
                } else {
                    Op op = TransformFilterPlacement.transform(exprs, gn, bgp);
                    return plainExecute(op, (QueryIterator)input, execCxt);
                }
            }
        }
    }

    private static BasicPattern reorder(BasicPattern pattern, QueryIterPeek peek, ReorderTransformation transform) {
        if (transform != null) {
            if (!peek.hasNext()) {
                throw new ARQInternalErrorException("Peek iterator is already empty");
            }

            BasicPattern pattern2 = Substitute.substitute(pattern, peek.peek());
            ReorderProc proc = transform.reorderIndexes(pattern2);
            pattern = proc.reorder(pattern);
        }

        return pattern;
    }

    private static boolean isDefaultGraphStorage(Node gn) {
        return Objects.isNull(gn) || Quad.isDefaultGraph(gn);
    }

    private static QueryIterator plainExecute(Op op, QueryIterator input, ExecutionContext execCxt) {
        ExecutionContext ec2 = new ExecutionContext(execCxt);
        ec2.setExecutor(plainFactory);
        return QC.execute(op, input, ec2);
    }


    /**
     * This is a copy/pasta of the inner factory classes of {@link OpExecutorTDB2}.
     * The only difference with the original is the use of a different {@link PatternMatchTDB2}
     * that will create {@link PreemptScanIteratorQuad} that enable pausing/resuming of query execution.
     **/
    static OpExecutorFactory plainFactory = new OpExecutorPlainFactorySage();

    private static class OpExecutorPlainFactorySage implements OpExecutorFactory {
        @Override
        public OpExecutor create(ExecutionContext execCxt) {return new OpExecutorPlainSage(execCxt);}
    }

    private static class OpExecutorPlainSage extends OpExecutor {
        Predicate<Tuple<NodeId>> filter = null; // (TODO) filter

        public OpExecutorPlainSage(ExecutionContext execCxt) {
            super(execCxt);
            filter = QC2.getFilter(execCxt.getContext());
        }

        @Override
        public QueryIterator execute(OpBGP opBGP, QueryIterator input) {
            Graph g = execCxt.getActiveGraph();

            if ( g instanceof GraphTDB) {
                BasicPattern bgp = opBGP.getPattern();
                Explain.explain("Execute", bgp, execCxt.getContext());
                // Triple-backed (but may be named as explicit default graph).
                GraphTDB gtdb = (GraphTDB)g;
                Node gn = decideGraphNode(gtdb.getGraphName(), execCxt);
                // return PatternMatchTDB2.execute(gtdb.getDSG(), gn, bgp, input, filter, execCxt);
                return PatternMatchSage.matchTriplePattern(bgp, input, execCxt);
            }
            Log.warn(this, "Non-GraphTDB passed to OpExecutorPlainSage: "+g.getClass().getSimpleName());
            return super.execute(opBGP, input);
        }

        @Override
        public QueryIterator execute(OpQuadPattern opQuadPattern, QueryIterator input) {
            Node gn = opQuadPattern.getGraphNode();
            gn = decideGraphNode(gn, execCxt);

            if ( execCxt.getDataset() instanceof DatasetGraphTDB) {
                DatasetGraphTDB ds = (DatasetGraphTDB)execCxt.getDataset();
                Explain.explain("Execute", opQuadPattern.getPattern(), execCxt.getContext());
                BasicPattern bgp = opQuadPattern.getBasicPattern();
                // return PatternMatchTDB2.execute(ds, gn, bgp, input, filter, execCxt);
                return PatternMatchSage.matchQuadPattern(bgp, gn, input, execCxt);
            }
            // Maybe a TDB named graph inside a non-TDB dataset.
            Graph g = execCxt.getActiveGraph();
            if ( g instanceof GraphTDB ) {
                // Triples graph from TDB (which is the default graph of the dataset),
                // used a named graph in a composite dataset.
                BasicPattern bgp = opQuadPattern.getBasicPattern();
                Explain.explain("Execute", bgp, execCxt.getContext());
                // Don't pass in G -- gn may be different.
                // return PatternMatchTDB2.execute(((GraphTDB)g).getDSG(), gn, bgp, input, filter, execCxt);
                // (TODO) double check this part, possibly throw for now
                return PatternMatchSage.matchQuadPattern(bgp, gn, input, execCxt);
            }
            Log.warn(this, "Non-DatasetGraphTDB passed to OpExecutorPlainTDB");
            return super.execute(opQuadPattern, input);
        }
    }

}
