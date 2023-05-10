package fr.gdd.sage.arq;

import org.apache.jena.graph.Graph;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.tdb2.store.GraphTDB;
import org.apache.jena.tdb2.store.GraphViewSwitchable;
import org.apache.jena.tdb2.solver.PatternMatchSage;



/**
 * Class in charge of creating preemptive basic graph patterns
 * (BGPs).
 *
 * Mostly comes from {@link org.apache.jena.tdb2.solver.StageGeneratorDirectTDB}
 **/
public class StageGeneratorSage implements StageGenerator {
    StageGenerator parent;
    
    public StageGeneratorSage(StageGenerator parent) {
        this.parent = parent;
    }
    
    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        // (TODO) everything goes by {@link OpExecutorSage} to (i) set up the context properly
        //  and avoid duplicated code

        Graph g = execCxt.getActiveGraph();
        if (g instanceof GraphViewSwitchable) {
            GraphViewSwitchable gvs = (GraphViewSwitchable)g;
            g = gvs.getBaseGraph();
        }
        if (!(g instanceof GraphTDB)) {
            return parent.execute(pattern, input, execCxt);
        }
        
        return PatternMatchSage.matchTriplePattern(pattern, input, execCxt);
    }

}
