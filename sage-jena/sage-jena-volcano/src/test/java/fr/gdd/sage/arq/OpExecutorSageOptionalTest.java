package fr.gdd.sage.arq;

import fr.gdd.sage.InMemoryInstanceOfTDB2;
import fr.gdd.sage.InMemoryInstanceOfTDB2ForOptional;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static fr.gdd.sage.arq.OpExecutorSageBGPTest.log;
import static fr.gdd.sage.arq.OpExecutorSageBGPTest.run_to_the_limit;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OpExecutorSageOptionalTest {

    static Dataset datasetWatdiv = null;
    static Dataset datasetOption = null;

    @BeforeAll
    public static void initializeDB() {
        datasetWatdiv = new InMemoryInstanceOfTDB2().getDataset();
        datasetOption = new InMemoryInstanceOfTDB2ForOptional().getDataset();
        // set up the chain of execution to use Sage when called on this dataset
        QC.setFactory(datasetWatdiv.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QC.setFactory(datasetOption.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineRegistry.addFactory(QueryEngineSage.factory);
    }

    @AfterAll
    public static void closeDB() {
        datasetWatdiv.abort();
        datasetOption.abort();
        TDBInternal.expel(datasetWatdiv.asDatasetGraph());
        TDBInternal.expel(datasetOption.asDatasetGraph());
    }


    @Test
    public void simple_optional_over_bgps() {
        Query query = QueryFactory.create("SELECT * WHERE {?s ?p ?o  OPTIONAL {?x ?y ?z FILTER (?z > 12)}}");
        try (QueryExecution qExec = QueryExecution.create()
                .query(query)
                .dataset(datasetWatdiv)
                .set(ARQ.symLogExec, Explain.InfoLevel.ALL).build()) {
            ResultSet rs = qExec.execSelect() ;
        }

        Op op = SSE.parseOp("(conditional " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (?s <http://www.geonames.org/ontology#doesNotExist> ?o))" + // never true
                ")");

        SageOutput<?> output = run_to_the_limit(datasetWatdiv, op, new SageInput<>());
        assertEquals(2, output.size());
    }

    @Test
    public void simple_optional_with_preemption_in_the_middle_of_the_two() {
        // does not need any remembering since it goes to the end of the first bgp
        // at the first result.
        Op op = SSE.parseOp("(conditional " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (?s <http://www.geonames.org/ontology#doesNotExist> ?o))" + // never true
                ")");
        SageOutput output = run_to_the_limit(datasetWatdiv, op, new SageInput<>().setLimit(1));
        assertEquals(1, output.size());
        output = run_to_the_limit(datasetWatdiv, op, new SageInput<>().setState(output.getState()));
        assertEquals(1, output.size());
    }

    @Test
    public void optional_that_stops_in_the_middle_with_a_result_before_stopping_and_none_after() {
        // so first it returns a solution <A> <OptionalB>, then pause/resume, then does not find
        // any <OptionalB>, yet, it must not return <A> alone since it returned one before
        String opAsString = "(conditional " +
                "(bgp (?s <http://address> <http://nantes>)) " +
                "(filter (!= ?o <http://cat>) (" +
                "    bgp (?s <http://own> ?o)"+
                "))" +
                ")";

        Op op = SSE.parseOp(opAsString);

        SageOutput output = run_to_the_limit(datasetOption, op, new SageInput<>());
        // they are two to live in Nantes but only one is friend with a snake and dog
        assertEquals(3, output.size());

        log.debug("Check snake and dog precede cat (in debug mode)");
        Op opCatsAndDogs = SSE.parseOp("(bgp (<http://Alice> <http://own> ?animal))");
        run_to_the_limit(datasetOption, opCatsAndDogs, new SageInput<>());

        op = SSE.parseOp(opAsString);
        output = run_to_the_limit(datasetOption, op, new SageInput<>().setLimit(1));
        assertEquals(1, output.size()); // carol with no animals

        output = run_to_the_limit(datasetOption, op, new SageInput<>().setLimit(1).setState(output.getState()));
        assertEquals(1, output.size()); // alice with snake

        output = run_to_the_limit(datasetOption, op, new SageInput<>().setLimit(1).setState(output.getState()));
        assertEquals(1, output.size()); // alice with dog

        output = run_to_the_limit(datasetOption, op, new SageInput<>().setState(output.getState()));
        // Without changes, the iterator will return something wrong which is Alice without animals because
        // at this pause/resume, the exploration starts after `dog`. Since it's optional, it will still return
        // Alice… The `optional` must remember that it saw an animal before pausing.
        // [main] DEBUG fr.gdd.sage.arq.OpExecutorSageBGPTest - ( ?s/[0x              56] = <http://Alice> ) -> [Root]
        assertEquals(0, output.size());
    }


}