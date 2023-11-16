package fr.gdd.sage.arq;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static fr.gdd.sage.arq.OpExecutorSageBGPTest.run_to_the_limit;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OpExecutorSageUnionTest {

    static Dataset dataset = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2().getDataset();

        // set up the chain of execution to use Sage when called on this dataset
        QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineRegistry.addFactory(QueryEngineSage.factory);
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }


    @Test
    public void simple_union_over_bgps() {
        Op op = SSE.parseOp("(union " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City0> <http://www.geonames.org/ontology#parentCountry> ?o))" +
                ")");

        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(3, output.size());
    }

    @Test
    public void preempt_at_every_step_of_union_2_1() {
        Op op = SSE.parseOp("(union " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City0> <http://www.geonames.org/ontology#parentCountry> ?o))" +
                ")");

        SageOutput output = run_to_the_limit(dataset, op, new SageInput<>().setLimit(1));
        assertEquals(1, output.size());
        // We are still in the first part of the union
        output = run_to_the_limit(dataset, op, new SageInput<>().setLimit(1).setState((output.getState())));
        assertEquals(1, output.size());
        // We are inbetween the first and second part of union
        output = run_to_the_limit(dataset, op, new SageInput<>().setState(output.getState()));
        assertEquals(1, output.size());
    }

    @Test
    public void preempt_with_union_of_union() {
        String query = "(union (bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>))(union " +
                "(bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City0> <http://www.geonames.org/ontology#parentCountry> ?o))" +
                "))";
        Op op = SSE.parseOp(query);
        SageOutput output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(2 + 2 + 1, output.size());

        output = new SageOutput();
        for (int i = 0; i  < 4; ++i) {
           output = run_to_the_limit(dataset, op, new SageInput<>().setState(output.getState()).setLimit(1));
           assertEquals(1, output.size());
        }
        output = run_to_the_limit(dataset, op, new SageInput<>().setState(output.getState()));
        assertEquals(1, output.size());
    }

    @Test
    public void union_in_bgp_so_it_is_called_by_a_join_and_have_a_nextStage() {
        String query = "(join (bgp (?s ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>))(union " +
                "(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City0> ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>)) " +
                "(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City0> <http://www.geonames.org/ontology#parentCountry> ?o))" +
                "))";
        Op op = SSE.parseOp(query);
        SageOutput output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(2, output.size());


        output = run_to_the_limit(dataset, op, new SageInput<>().setLimit(1));
        assertEquals(1, output.size());
        output = run_to_the_limit(dataset, op, new SageInput<>().setState(output.getState()));
        assertEquals(1, output.size());
    }
}