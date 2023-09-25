package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.solver.OpExecutorTDB2;
import org.apache.jena.tdb2.solver.QueryEngineTDB;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * Debugging/Profiling utility of the long query `Query10122` of the `watdiv`
 * benchmark. It returns a lot of results, so when executing Sage after TDB,
 * I experienced 24s/op vs 36s/op possibly due to garbage collection.
 **/
class Query10122Test {
    private static Logger log = LoggerFactory.getLogger(Query10122Test.class);

    static Dataset dataset;

    static OpExecutorFactory opExecutorTDB2ForceOrderFactory;

    String query = "SELECT ?v6 ?v8 ?v0 ?v7 ?v3 ?v4 ?v1 ?v2 WHERE {\n" +
            "\t?v1 <http://schema.org/priceValidUntil> ?v8.\n" +
            "\t?v1 <http://purl.org/goodrelations/validFrom> ?v2.\n" +
            "\t?v1 <http://purl.org/goodrelations/validThrough> ?v3.\n" +
            "\t?v1 <http://schema.org/eligibleQuantity> ?v6.\n" +
            "\t?v0 <http://purl.org/goodrelations/offers> ?v1.\n" +
            "\t?v1 <http://schema.org/eligibleRegion> ?v7.\n" +
            "\t?v4 <http://schema.org/nationality> ?v7.\n" +
            "\t?v4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://db.uwaterloo.ca/~galuc/wsdbm/Role0>.\n" +
            "}";

    @BeforeAll
    public static void initialize_database() {
        Field plainFactoryField = ReflectionUtils._getField(OpExecutorTDB2.class, "plainFactory");
        opExecutorTDB2ForceOrderFactory = (OpExecutorFactory) ReflectionUtils._callField(plainFactoryField, OpExecutorTDB2.class, null);

        Optional dirPath_opt = Optional.of("../datasets");
        Watdiv10M watdiv = new Watdiv10M(dirPath_opt); // creates the db if need be
        dataset = TDB2Factory.connectDataset(watdiv.dbPath_asStr);
        dataset.begin(ReadWrite.READ);
    }

    @AfterAll
    public static void close_database() {
        dataset.end();
    }

    @Test
    public void execute_query_1000_with_TDB() {
        QC.setFactory(dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
        QueryEngineTDB.register();

        Context c = dataset.getContext().copy();

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long nbResults = 0;
        ResultSet rs = queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults+=1;
        }

        log.info("Got {} results…", nbResults);
    }

    @Test
    public void execute_query_1000_with_TDB_force_order() {
        QC.setFactory(dataset.getContext(), opExecutorTDB2ForceOrderFactory);
        QueryEngineTDB.register();

        Context c = dataset.getContext().copy();
        c.set(ARQ.optimization, false);

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long nbResults = 0;
        ResultSet rs = queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults+=1;
        }

        log.info("Got {} results…", nbResults);
    }

    @Test
    public void execute_query_1000_with_Sage_force_order() {
        QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineSage.register();

        SageInput<?> input = new SageInput<>();
        // Context c = dataset.getContext().set(SageConstants.input, input);
        dataset.getContext().set(ARQ.optimization, false);
        /*dataset.getContext().set(SageConstants.timeout, 1000);

        System.out.println("FIRST");
        Pair results = ExecuteUtils.executeTillTheEnd(dataset, query);


        log.info("Got {} results in {} pause(s)/resume(s)…", results.left, results.right);*/

        dataset.getContext().set(SageConstants.timeout, 1);

        System.out.println("SECOND");
        Pair results = ExecuteUtils.executeTillTheEnd(dataset, query);

        log.info("Got {} results in {} pause(s)/resume(s)…", results.left, results.right);
    }

    @Test
    public void execute_query_1000_with_Sage_with_TDB_order() {
        QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineSage.register();

        SageInput<?> input = new SageInput<>();
        Context c = dataset.getContext().copy().set(SageConstants.input, input);
        // c.set(ARQ.optimization, false);

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long nbResults = 0;
        ResultSet rs = queryExecution.execSelect() ;
        while (rs.hasNext()) {
            rs.next();
            nbResults+=1;
        }

        log.info("Got {} results…", nbResults);
    }

}