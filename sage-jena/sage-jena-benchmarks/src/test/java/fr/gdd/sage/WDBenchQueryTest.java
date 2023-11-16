package fr.gdd.sage;

import fr.gdd.sage.arq.OpExecutorSage;
import fr.gdd.sage.arq.QueryEngineSage;
import fr.gdd.sage.arq.SageConstants;
import fr.gdd.sage.databases.persistent.WDBench;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.io.SageInput;
import org.apache.jena.query.*;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.mgt.Explain;
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

class WDBenchQueryTest {
    private static Logger log = LoggerFactory.getLogger(WDBenchQueryTest.class);

    static Dataset dataset;

    static OpExecutorFactory opExecutorTDB2ForceOrderFactory;

    /* String query = "SELECT  * WHERE { " +
            "?x1  <http://www.wikidata.org/prop/direct/P31>  <http://www.wikidata.org/entity/Q20650540> " +
            "OPTIONAL { ?x1  <http://www.wikidata.org/prop/direct/P921>  ?x2 } "+
            "} LIMIT 100000";*/

    String query = "SELECT * WHERE {?x5 <http://www.wikidata.org/prop/direct/P171> <http://www.wikidata.org/entity/Q10744387>} LIMIT 1000000";

    @BeforeAll
    public static void initialize_database() {
        Field plainFactoryField = ReflectionUtils._getField(OpExecutorTDB2.class, "plainFactory");
        opExecutorTDB2ForceOrderFactory = (OpExecutorFactory) ReflectionUtils._callField(plainFactoryField, OpExecutorTDB2.class, null);

        WDBench wdbench = new WDBench(Optional.of("../datasets")); // creates the db if need be
        dataset = TDB2Factory.connectDataset(wdbench.dbPath_asStr);
        dataset.begin(ReadWrite.READ);
    }

    @AfterAll
    public static void close_database() {
        dataset.end();
    }

    @Test
    public void execute_with_TDB() {
        QC.setFactory(dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
        QueryEngineTDB.register();

        Context c = dataset.getContext().copy();

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query)
                    .set(ARQ.symLogExec, Explain.InfoLevel.ALL).build();
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
    public void execute_with_TDB_force_order() {
        QC.setFactory(dataset.getContext(), opExecutorTDB2ForceOrderFactory);
        QueryEngineTDB.register();

        Context c = dataset.getContext().copy();
        c.set(ARQ.optimization, false);

        QueryExecution queryExecution = null;
        try {
            queryExecution = QueryExecution.create()
                    .dataset(dataset)
                    .context(c)
                    .query(query)
                    .set(ARQ.symLogExec, Explain.InfoLevel.ALL).build();
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
    public void execute_with_Sage_force_order() {

        QC.setFactory(dataset.getContext(), new OpExecutorSage.OpExecutorSageFactory(ARQ.getContext()));
        QueryEngineSage.register();

        SageInput<?> input = new SageInput<>();
        // Context c = dataset.getContext().set(SageConstants.input, input);
        dataset.getContext().set(ARQ.optimization, false);
        dataset.getContext().set(SageConstants.timeout, 1000);

        System.out.println("FIRST");
        Pair results = ExecuteUtils.executeTillTheEnd(dataset, query);


        log.info("Got {} results in {} pause(s)/resume(s)…", results.left, results.right);

        dataset.getContext().set(SageConstants.timeout, 1);

        System.out.println("SECOND");
        results = ExecuteUtils.executeTillTheEnd(dataset, query);

        log.info("Got {} results in {} pause(s)/resume(s)…", results.left, results.right);
    }

    @Test
    public void execute_with_Sage_and_TDB_order() {
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
