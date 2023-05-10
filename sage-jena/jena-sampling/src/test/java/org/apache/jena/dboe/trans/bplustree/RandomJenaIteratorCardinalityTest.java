package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.InMemoryInstanceOfTDB2ForRandom;
import fr.gdd.sage.OpExecutorRandom;
import fr.gdd.sage.QueryEngineRandom;
import fr.gdd.sage.RandomScanIteratorFactory;
import fr.gdd.sage.configuration.SageServerConfiguration;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.atlas.lib.tuple.TupleFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.iterator.PreemptScanIteratorTupleId;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.solver.BindingNodeId;
import org.apache.jena.tdb2.solver.PreemptStageMatchTuple;
import org.apache.jena.tdb2.store.DatasetGraphTDB;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.store.nodetupletable.NodeTupleTable;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RandomJenaIteratorCardinalityTest {

    Logger log = LoggerFactory.getLogger(RandomJenaIteratorCardinalityTest.class);

    static Dataset dataset;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2ForRandom().getDataset();
        QueryEngineRandom.register();
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    @Disabled
    @Test
    public void cardinality_of_nothing() {
        OpBGP op = (OpBGP) SSE.parseOp("(bgp (?s ?p <http://licorne>))");
        RandomJenaIterator it = getRandomJenaIterator(op);

        assertEquals(0, it.cardinality());
    }

    @Disabled
    @Test
    public void cardinality_of_a_one_tuple_triple_pattern() {
        OpBGP op = (OpBGP) SSE.parseOp("(bgp (?s ?p <http://cat>))");
        RandomJenaIterator it = getRandomJenaIterator(op);

        assertEquals(1, it.cardinality());
    }

    @Disabled
    @Test
    public void cardinality_of_a_few_tuples_triple_pattern() {
        OpBGP op = (OpBGP) SSE.parseOp("(bgp (<http://Alice> ?p ?o))");
        RandomJenaIterator it = getRandomJenaIterator(op);
        assertEquals(4, it.cardinality());
    }

    @Disabled
    @Test
    public void cardinality_of_larger_triple_pattern_above_leaf_size() {

    }


    public static RandomJenaIterator getRandomJenaIterator(OpBGP op) {
        DatasetGraphTDB activeGraph = TDBInternal.getDatasetGraphTDB(dataset);

        ExecutionContext execCxt = new ExecutionContext(
                dataset.getContext(),
                dataset.asDatasetGraph().getDefaultGraph(),
                activeGraph,
                new OpExecutorRandom.OpExecutorRandomFactory(dataset.getContext()));

        Tuple<Node>  patternTuple = TupleFactory.create3(
                op.getPattern().get(0).getSubject(),
                op.getPattern().get(0).getPredicate(),
                op.getPattern().get(0).getObject());

        NodeId ids[] = new NodeId[patternTuple.len()]; // ---- Convert to NodeIds
        final Var[] vars = new Var[patternTuple.len()]; // Variables for this tuple after substitution

        NodeTupleTable nodeTupleTable = activeGraph.getTripleTable().getNodeTupleTable();
        PreemptStageMatchTuple.prepare(nodeTupleTable.getNodeTable(), patternTuple, BindingNodeId.root, ids, vars);

        RandomScanIteratorFactory f = new RandomScanIteratorFactory(execCxt);
        return (RandomJenaIterator) f.getScan(nodeTupleTable, TupleFactory.create(ids), 12);
    }


}