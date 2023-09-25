package fr.gdd.sage;

import fr.gdd.sage.databases.persistent.Watdiv10M;
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
 * Useful for debugging the same pattern of {@link SimplePatternBenchmark}.
 * Useful for profiling as well.
 * Indeed,`jmh` starts new VMs precluding the use of debug/profiling mode…
 **/
class SimplePatternBenchmarkTest {
    private static Logger log = LoggerFactory.getLogger(SimplePatternBenchmarkTest.class);

    static Dataset dataset;

    static OpExecutorFactory opExecutorTDB2ForceOrderFactory;

    @BeforeAll
    public static void initialize_database() {
        Field plainFactoryField = ReflectionUtils._getField(OpExecutorTDB2.class, "plainFactory");
        opExecutorTDB2ForceOrderFactory = (OpExecutorFactory) ReflectionUtils._callField(plainFactoryField, OpExecutorTDB2.class, null);

        Optional dirPath_opt = Optional.of("target");
        Watdiv10M watdiv = new Watdiv10M(dirPath_opt); // creates the db if need be
        dataset = TDB2Factory.connectDataset(watdiv.dbPath_asStr);
        dataset.begin(ReadWrite.READ);
    }

    @AfterAll
    public static void close_database() {
        dataset.end();
    }

    @Test
    public void execute_vpo_with_TDB() {
        String query = "SELECT * WHERE {" +
                "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender0>"+
                "}";

        QC.setFactory(dataset.getContext(), OpExecutorTDB2.OpExecFactoryTDB);
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

}