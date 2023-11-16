package fr.gdd.sage;

import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.generics.Pair;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Optional;

/**
 * Queries from benchmarks usually comprise specific SPARQL operations. For
 * instance, `watdiv` only has joins over triples; `jobrdf` has joins, and
 * filters.
 *
 * This benchmark aims to evaluate the cost of simple single triple patterns,
 * once again with preemptive volcano against simple volcano.
 */
// @BenchmarkMode({Mode.SingleShotTime})
@State(Scope.Benchmark)
@Warmup(time = 5, iterations = 5)
@Fork(2)
@Measurement(iterations = 1)
public class SimplePatternBenchmark {
    final static Logger log = LoggerFactory.getLogger(SimplePatternBenchmark.class);

    @Param({EngineTypes.SageForceOrderTimeout1ms, EngineTypes.TDB, EngineTypes.Sage, EngineTypes.TDBForceOrder, EngineTypes.SageForceOrder,
            EngineTypes.SageForceOrderLimit1, EngineTypes.SageForceOrderTimeout1ms})
    public String b_engine;

    @Param("target/watdiv10M")
    public String z_dbPath;

    static HashMap<String, Long> nbResultsPerQuery = new HashMap<>();

    @Param({ "?v0 <http://db.uwaterloo.ca/~galuc/wsdbm/gender> <http://db.uwaterloo.ca/~galuc/wsdbm/Gender0>", // vPO
             "?v0 <http://xmlns.com/foaf/familyName> ?v1." }) // vPv
    public String a_pattern; // prefixed with alphanumeric character to force the execution order of @Param

    @Setup(Level.Trial)
    public void setup(SetupBenchmark.BenchmarkExecutionContext ec) {
        try {
            SetupBenchmark.setup(ec, z_dbPath, b_engine);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @TearDown(Level.Trial)
    public void setdown(SetupBenchmark.BenchmarkExecutionContext ec) {
        SetupBenchmark.setdown(ec, b_engine);
    }

    @Setup(Level.Trial)
    public void read_query(SetupBenchmark.BenchmarkExecutionContext ec) {
        ec.query = "SELECT * WHERE {" + a_pattern + "}";
        log.debug("{}", ec.query);
    }

    @Benchmark
    public long execute(SetupBenchmark.BenchmarkExecutionContext ec) throws Exception {
        Pair<Long, Long> nbResultsAndPreempt = SetupBenchmark.execute(ec, b_engine);
        log.debug("Got {} results for this query.", nbResultsAndPreempt.left);

        if (nbResultsPerQuery.containsKey(ec.query)) {
            long previousNbResults = nbResultsPerQuery.get(ec.query);
            if (previousNbResults != nbResultsAndPreempt.left) {
                throw (new Exception(String.format("/!\\ not the same number of results on %s: %s vs %s.",
                        ec.query, previousNbResults, nbResultsAndPreempt.left)));
            }
        } else {
            nbResultsPerQuery.put(ec.query, nbResultsAndPreempt.left);
        }

        return nbResultsAndPreempt.left;
    }

    public static void main(String[] args) throws RunnerException {
        Optional<String> dirPath_opt = (args.length > 0) ? Optional.of(args[0]) : Optional.empty();

        Watdiv10M watdiv = new Watdiv10M(dirPath_opt); // creates the db if need be

        Options opt = new OptionsBuilder()
                .include(SimplePatternBenchmark.class.getSimpleName())
                .param("z_dbPath", watdiv.dbPath_asStr)
                .threads(1) // (TODO) manage to up this number, for now, `Maximum lock count exceeded`… or other
                .result("target/SimplePatternBenchmark.csv")
                .resultFormat(ResultFormatType.CSV)
                .build();

        new Runner(opt).run();


    }
}
