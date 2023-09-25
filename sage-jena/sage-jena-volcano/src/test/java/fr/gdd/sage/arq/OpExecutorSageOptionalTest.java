package fr.gdd.sage.arq;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2;
import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2ForOptional;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.sparql.sse.SSE;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

import static fr.gdd.sage.arq.OpExecutorSageBGPTest.run_to_the_limit;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OpExecutorSageOptionalTest {

    private static Logger log = LoggerFactory.getLogger(OpExecutorSageOptionalTest.class);

    static Dataset datasetWatdiv = null;
    static Dataset datasetOption = null;

    @BeforeAll
    public static void initializeDB() {
        datasetWatdiv = new InMemoryInstanceOfTDB2().getDataset();
        datasetOption = new InMemoryInstanceOfTDB2ForOptional().getDataset();
        // set up the chain of execution to use Sage when called on this dataset
        QC.setFactory(datasetWatdiv.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QC.setFactory(datasetOption.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineSage.register();
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
            ResultSet rs = qExec.execSelect();
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
        String opAsString = "(leftjoin " +
                "(bgp (?s <http://address> <http://nantes>)) " +
                "(filter (!= ?o <http://cat>) (" +
                "    bgp (?s <http://own> ?o)" +
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

    @Test
    public void one_optional_after_the_other() {
        String query_asString = "SELECT * WHERE " +
                "{?person <http://address> ?address " + // for all address
                "OPTIONAL {?person2 <http://address> ?address}" + // get all people2
                "OPTIONAL {?person2 <http://address> ?address2 . ?person2 <http://own> ?animal } }"; // all these people2 have animal ?
        Query query = QueryFactory.create(query_asString);
        Op op = Algebra.compile(query);

        // A -> no address (NULL) ->

        List<String> expectedSolutions = new ArrayList<>();
        try (QueryExecution qExec = QueryExecution.create()
                .query(query)
                .dataset(datasetOption)
                .set(ARQ.symLogExec, Explain.InfoLevel.ALL).build()) {
            ResultSet rs = qExec.execSelect();
            while (rs.hasNext()) {
                expectedSolutions.add(rs.next().toString());
            }
            log.debug("Expected result:");
            expectedSolutions.forEach(s -> log.debug(s));
        }

        List<String> actualSolutions = new ArrayList<>();
        Pair<SageOutput, List<String>> output_and_results = run_to_the_limit_and_get_results(datasetOption, op, new SageInput<>().setLimit(1));
        SageOutput output = output_and_results.getLeft();
        actualSolutions.addAll(output_and_results.getRight());
        while (Objects.nonNull(output) && Objects.nonNull(output.getState()) && output.size() > 0) {
            output_and_results = run_to_the_limit_and_get_results(datasetOption, op, new SageInput<>().setState(output.getState()).setLimit(1));
            output = output_and_results.getLeft();
            actualSolutions.addAll(output_and_results.getRight());
        }
        log.debug("Got results:");
        actualSolutions.forEach(s -> log.debug(s));

        assertEquals(expectedSolutions.size(), actualSolutions.size());
        assertEquals(new TreeSet<>(expectedSolutions), new TreeSet<>(actualSolutions));
    }

    public static Pair<SageOutput, List<String>> run_to_the_limit_and_get_results(Dataset dataset, Op query, SageInput<?> input) {
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

        List<String> solutions = new ArrayList<>();
        long nb_results = 0;
        while (it.hasNext()) {
            Binding b = it.next();
            solutions.add(b.toString());
            log.debug(b.toString());
            nb_results += 1;
        }

        SageOutput<SerializableRecord> output = c.get(SageConstants.output);
        if (limitIsSet) {
            assertEquals(input.getLimit(), nb_results);
            assertEquals(input.getLimit(), output.size());
        }
        return new Pair<>(output, solutions);
    }

}