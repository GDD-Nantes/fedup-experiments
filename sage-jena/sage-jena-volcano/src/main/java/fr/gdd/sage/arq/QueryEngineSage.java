package fr.gdd.sage.arq;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpLib;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.*;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.*;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instead of relying on {@link org.apache.jena.tdb2.solver.QueryEngineTDB} to
 * call our {@link OpExecutorSage}, we create and register our engine. We add a necessary
 * counter in top of the execution pipeline.
 */
public class QueryEngineSage extends QueryEngineTDB {

    private static Logger log = LoggerFactory.getLogger(QueryEngineSage.class);

    protected QueryEngineSage(Op op, DatasetGraphTDB dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    protected QueryEngineSage(Query query, DatasetGraphTDB dataset, Binding input, Context cxt) {
        super(query, dataset, input, cxt);
    }

    static public void register() {
        QueryEngineRegistry.addFactory(QueryEngineSage.factory);
    }

    static public void unregister() {
        QueryEngineRegistry.removeFactory(QueryEngineSage.factory);
    }

    private static boolean isUnionDefaultGraph(Context cxt) {
        return cxt.isTrue(TDB2.symUnionDefaultGraph1) || cxt.isTrue(TDB2.symUnionDefaultGraph2);
    }

    @Override
    public QueryIterator eval(Op op, DatasetGraph dsg, Binding input, Context context) {
        // (TODO) put Sage Optimizer for join order here, or copy TDB Order as well.

        // #1 an explain that comes from {@link QueryEngineTDB}
        if ( isUnionDefaultGraph(context) && ! isDynamicDataset() ) {
            op = OpLib.unionDefaultGraphQuads(op) ;
            Explain.explain("REWRITE(Union default graph)", op, context);
        }

        // #2 comes from {@link QueryEngineBase}
        ExecutionContext execCxt = new ExecutionContext(context, dsg.getDefaultGraph(), dsg, QC.getFactory(context));
        IdentifierLinker.create(execCxt, op, true);

        QueryIterator qIter1 =
                ( input.isEmpty() ) ? QueryIterRoot.create(execCxt)
                        : QueryIterRoot.create(input, execCxt);
        QueryIterator qIter = QC.execute(op, qIter1, execCxt);

        // #3 in between we add our home-made counter iterator :)
        PreemptCounterIter counterIter = new PreemptCounterIter(qIter, execCxt);



        // Wrap with something to check for closed iterators.
        qIter = QueryIteratorCheck.check(counterIter, execCxt);
        // Need call back.
        if ( context.isTrue(ARQ.enableExecutionTimeLogging) )
            qIter = QueryIteratorTiming.time(qIter);

        // #4 wraps it in a root that handle pause exceptions
        PreemptRootIter preemptRootIter = new PreemptRootIter(qIter, execCxt);
        return preemptRootIter;
    }

    // ---- Factory
    public static QueryEngineFactory factory = new QueryEngineSage.QueryEngineFactorySage();

    /**
     * Mostly identical to {@link org.apache.jena.tdb2.solver.QueryEngineTDB.QueryEngineFactoryTDB}
     * but calling {@link QueryEngineSage} instead of {@link QueryEngineTDB} to build plans.
     */
    public static class QueryEngineFactorySage extends QueryEngineFactoryTDB {

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding input, Context context) {
            QueryEngineSage engine = new QueryEngineSage(query, dsgToQuery(dataset), input, context);
            return engine.getPlan();
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding binding, Context context) {
            QueryEngineSage engine = new QueryEngineSage(op, dsgToQuery(dataset), binding, context);
            return engine.getPlan();
        }
    }
}
