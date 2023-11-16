package fr.gdd.sage.arq;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.VarUtils;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Testing the executor by building queries by hand.
 */
public class OpExecutorSageBGPTest {

    private static Logger log = LoggerFactory.getLogger(OpExecutorSageBGPTest.class);

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
    // related to issue#7
    public void simple_select_all_triples() {
        Op op = SSE.parseOp("(bgp (?s ?p ?o))");

        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(10, output.size());
    }

    @Test
    public void select_a_singleton_ie_a_fully_bounded_triple() {
        Op op = SSE.parseOp("(bgp (" +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City0> " +
                "<http://www.geonames.org/ontology#parentCountry> " +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/Country6>))");
        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(1, output.size());
    }

    @Test
    // related to issue#13
    public void select_a_singleton_and_preempt() {
        Op op = SSE.parseOp("(bgp (" +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City0> " +
                "<http://www.geonames.org/ontology#parentCountry> " +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/Country6>) (" +
                "?s <http://www.geonames.org/ontology#parentCountry> ?o))");
        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(10, output.size()); // make sure there are 10 results

        // now with preemption
        output = run_to_the_limit(dataset, op, new SageInput<>().setLimit(1));
        assertEquals(1, output.size());

        // the saved state for singleton is always null since it always
        // needs to produce its unique value on resuming.
        assertNull(output.getState().get(0));

        SageOutput<?> rest = run_to_the_limit(dataset, op, new SageInput().setState(output.getState()));
        assertEquals(9, rest.size());
    }

    @Test
    public void select_a_null_iterator() {
        Op op = SSE.parseOp("(bgp (" +
                "<http://deliberate_mistake> " +
                "<http://www.geonames.org/ontology#parentCountry> " +
                "<http://db.uwaterloo.ca/~galuc/wsdbm/Country6>))");
        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(0, output.size());
        // we don't need for preemptive testing since null always immediately
        // report `hasNext` false.
    }

    @Test
    public void simple_select_all_triples_by_predicate() {
        Op op = SSE.parseOp("(bgp (?s <http://www.geonames.org/ontology#parentCountry> ?o))");

        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>());
        assertEquals(10, output.size());
    }

    @Test
    public void select_all_triple_but_pauses_at_first_then_resume() {
        Op op = SSE.parseOp("(bgp (?s <http://www.geonames.org/ontology#parentCountry> ?o))");

        // #A we set a limit of only one result on first execution
        SageOutput<?> output = run_to_the_limit(dataset, op, new SageInput<>().setLimit(1));

        // #B Then we don't set a limit to get the other 9 results
        // thanks to `output.getState()`, the iterator is able to skip where the previous paused its execution
        SageOutput<?> rest = run_to_the_limit(dataset, op, new SageInput().setState(output.getState()));
        assertEquals(9, rest.size());
    }

    @Test
    public void simple_bgp_then_pause_at_first_then_resume() {
        Op op = SSE.parseOp("(bgp (<http://db.uwaterloo.ca/~galuc/wsdbm/City102> ?p <http://db.uwaterloo.ca/~galuc/wsdbm/Country17>)" +
                " (?s <http://www.geonames.org/ontology#parentCountry> ?o))");

        SageOutput<?> out = run_to_the_limit(dataset, op, new SageInput<>().setLimit(1));
        SageOutput<?> rest = run_to_the_limit(dataset, op, new SageInput().setState(out.getState()));
        assertEquals(9, rest.size());
    }

    @Test
    public void a_simple_bgp_with_a_limit () {
        String query_asString = "SELECT * WHERE {?s ?p ?o} LIMIT 5";
        Query query = QueryFactory.create(query_asString);
        Op op = Algebra.compile(query);

        SageOutput output = run_to_the_limit(dataset, op, new SageInput<>().setLimit(1));
        assertEquals(1, output.size());
        output = run_to_the_limit(dataset, op, new SageInput<>().setState(output.getState()).setLimit(1));
        assertEquals(1, output.size());
        output = run_to_the_limit(dataset, op, new SageInput<>().setState(output.getState()).setLimit(1));
        assertEquals(1, output.size());
        output = run_to_the_limit(dataset, op, new SageInput<>().setState(output.getState()));
        // despite the fact that there are more results, it only returns 2 to complete the 3 already produced
        // and reach the LIMIT 5.
        assertEquals(2, output.size());
    }

    /* ***************************************************************************** */

    /**
     * Designed to easily profile query creation when the number of thread increases.
     * At the time of the writing (June 22th 2023), it highlights a locking mechanism
     * even in read-only mode that negatively impacts performance.
     */
    @Disabled
    @Test
    public void test_concurrent_execution_to_profile_perf() throws InterruptedException {
        Dataset dataset = TDB2Factory.connectDataset("../target/watdiv10M");

        int numberOfThreads = 1;
        ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        Op op = SSE.parseOp("(bgp (?s <http://db.uwaterloo.ca/~galuc/wsdbm/gender> ?o))");

        for (int i = 0; i < numberOfThreads; i++) {
            service.execute(() -> {
                QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
                QueryEngineRegistry.addFactory(QueryEngineSage.factory);

                final long TIMEOUT = 10000;
                final long DEADLINE = System.currentTimeMillis() + TIMEOUT;

                dataset.begin(ReadWrite.READ);
                long sum = 0;
                while (System.currentTimeMillis() < DEADLINE) {
                    run_to_the_limit(dataset, op, new SageInput<>().setLimit(1));
                    sum += 1;
                }
                // assertEquals(LIMIT, sum);

                log.info("Number of random walks performed by a thread: {}", sum);
                latch.countDown();
            });
        }
        latch.await();
    }


    /**
     * Runs the query until there are no result anymore.
     */
    public static SageOutput<SerializableRecord> run_to_the_limit(Dataset dataset, Op query, SageInput<?> input) {
        boolean limitIsSet = input.getLimit() != Long.MAX_VALUE;
        Context c = dataset.getContext().copy()
                // .set(SageConstants.input, input)
                .set(SageConstants.limit, input.getLimit())
                .set(SageConstants.timeout, input.getTimeout())
                .set(SageConstants.state, input.getState())
                // .set(SageConstants.output, new SageOutput<>())
                .set(ARQ.optimization, false); // we don't want reordering of triples for tests
        Plan plan = QueryEngineSage.factory.create(query, dataset.asDatasetGraph(), BindingRoot.create(), c);
        QueryIterator it = plan.iterator();

        long nb_results = 0;
        while (it.hasNext()) {
            Binding b = it.next();
            log.debug(b.toString());
            nb_results += 1;
        }

        SageOutput<SerializableRecord> output = c.get(SageConstants.output);
        if (limitIsSet) {
            assertEquals(input.getLimit(), nb_results);
            assertEquals(input.getLimit(), output.size());
        }
        return output;
    }

}