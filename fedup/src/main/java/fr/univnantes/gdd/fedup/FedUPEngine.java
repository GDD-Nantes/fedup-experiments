package fr.univnantes.gdd.fedup;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;


public class FedUPEngine extends QueryEngineMain {

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    static public void unregister() {
        QueryEngineRegistry.removeFactory(factory);
    }

    protected FedUPEngine(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    protected FedUPEngine(Query query, DatasetGraph dataset, Binding input, Context context) {
        super(query, dataset, input, context);
    }

    /* ***************************************************************** */

    static FedUPEngineFactory factory = new FedUPEngineFactory();

    protected static class FedUPEngineFactory implements QueryEngineFactory {
        protected FedUPEngineFactory() {
        }

        public boolean isFederated(Context context) {
            return context.isDefined(FedUPConstants.federation) || context.isDefined(FedUPConstants.summary);
        }

        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return isFederated(context);
        }

        public Plan create(Query query, DatasetGraph dataset, Binding input, Context context) {
            FedUPEngine engine = new FedUPEngine(query, dataset, input, context);
            return engine.getPlan();
        }

        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            return isFederated(context);
        }

        public Plan create(Op op, DatasetGraph dataset, Binding binding, Context context) {
            FedUPEngine engine = new FedUPEngine(op, dataset, binding, context);
            return engine.getPlan();
        }
    }
}
