package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageOutput;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.jena.base.Sys;
import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Aims to ease the simple execution of a query from start to finish
 * despite pause/resume.
 **/
public class ExecuteUtils {
    final static Logger log = LoggerFactory.getLogger(ExecuteUtils.class);

    static Integer expectedNumResults = null;

    static Set<String> solutions = null;

    /**
     * Execute a parsed query on a dataset until all results are produced.
     * @param dataset The dataset to execute on.
     * @param query The query to execute on the dataset.
     * @param withSerialize An optional serialization parameter that state whether time for serialize should be
     *                      included or not.
     * @return A pair (number of results, number of pauses)
     */
    public static Pair<Long, Long> executeQueryTillTheEnd(Dataset dataset, Query query, boolean... withSerialize) {
        long nbPreempt = -1; // the first execution is not a preempt
        long sum = 0;
        SageOutput<?> results = null;

        Map<Integer, Serializable> state = Map.of();
        byte[] serialized = null;
        Map<Integer, Serializable> previousState = null;

        //Set<String> preparingSolutions = new HashSet<>();

        while (Objects.isNull(results)|| (!Objects.isNull(results.getState()))) {
            nbPreempt += 1;

            if (Objects.nonNull(withSerialize) && withSerialize.length > 0 && withSerialize[0] && Objects.nonNull(serialized)) {
                SageOutput meow = SerializationUtils.deserialize(serialized);
                state = meow.getState();
            }

            Context c = dataset.getContext().copy().set(SageConstants.state, state);
            QueryExecution qe = null; //    QueryExecution qe = QueryExecutionFactory.create(query, dataset);

            try {
                qe = QueryExecution.create()
                        .dataset(dataset)
                        .context(c)
                        .query(query).build();
            } catch (Exception e) {
                e.printStackTrace();
            }

            // ResultSet result_set = new ResultSetSage(qe.execSelect());
            ResultSet result_set = qe.execSelect();

            while (result_set.hasNext()) { // must enumerate to actually execute
                QuerySolution solution = result_set.next();
                // System.out.println(solution);
                /*if (Objects.nonNull(solutions)) {
                    if (!solutions.contains(solution.toString())) {
                        results = qe.getContext().get(SageConstants.output);
                        state = (Map) results.getState();
                        System.out.println(state);
                        System.out.println("WRONG = " + solution.toString() );
                        throw new RuntimeException("WRONG");
                    }
                }*/
                /*if (preparingSolutions.contains(solution.toString())) {
                    results = qe.getContext().get(SageConstants.output);
                    state = (Map) results.getState();
                    System.out.println(state);
                    System.out.println("DOUBLON = " + solution.toString() );
                    throw new RuntimeException("DOUBLON");
                }
                preparingSolutions.add(solution.toString());*/

                sum += 1;
            }
            log.debug("Got {} results so far…" , sum);

            results = qe.getContext().get(SageConstants.output);
            // System.out.println(results.getState());

            if (Objects.nonNull(withSerialize) && withSerialize.length > 0 && withSerialize[0]) {
                serialized = SerializationUtils.serialize(results);
            }
            state = (Map) results.getState();
            qe.close();

            if (Objects.nonNull(state) && state.equals(previousState)) {
                System.out.println(":{");
            }

            log.debug("Saved state {}", results.getState());
        }


        /*if (Objects.isNull(solutions)) {
            solutions = preparingSolutions;
        }*/

        return new Pair<>(sum, nbPreempt);
    }

    /**
     * Execute a query represented as a string on a dataset until all results are produced.
     * @param dataset The dataset to execute on.
     * @param query The query to execute on the dataset.
     * @param withSerialize An optional serialization parameter that state whether time for serialize should be
     *                      included or not.
     * @return A pair (number of results, number of pauses)
     */
    public static Pair<Long, Long> executeTillTheEnd(Dataset dataset, String query, boolean... withSerialize) {
        Query q = QueryFactory.create(query);
        return executeQueryTillTheEnd(dataset, q, withSerialize);
    }

    /**
     * Execute a query represented as a string on a TDB2 dataset with a TDB2 query engine.
     * @param dataset The dataset to execute on.
     * @param query The query to execute on the dataset.
     * @return A pair (number of results, 0), since the number of pause is always 0.
     */
    public static Pair<Long, Long> executeTDB(Dataset dataset, String query) {
        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(dataset.getContext().copy())
                    .query(query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long nbResults = 0;
        ResultSet rs = queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults += 1;
        }

        log.debug("Got {} results for this query.", nbResults);

        return new Pair<>(nbResults, (long) 0);
    }


    public static void main(String[] args) {
        String path = "/Users/nedelec-b-2/Desktop/Projects/sage-jena/sage-jena-module/watdiv10M";
        Dataset dataset = TDB2Factory.connectDataset(path);
        dataset.begin();

        ARQ.getContext().set(SageConstants.limit, 1);
        QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineSage.register();
        
        String query_as_str = "SELECT ?o WHERE {<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer6> ?p ?o .}";
        Query query = QueryFactory.create(query_as_str);

        Pair resultsAndPreempt = executeQueryTillTheEnd(dataset, query);
        dataset.end();

        System.out.printf("Got %s results in %s pauses/resumes.\n", resultsAndPreempt.left, resultsAndPreempt.right);
    }
    
}
