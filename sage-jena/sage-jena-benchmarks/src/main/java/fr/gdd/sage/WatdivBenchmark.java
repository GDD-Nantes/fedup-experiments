package fr.gdd.sage;

import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.generics.Pair;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class WatdivBenchmark {
    final static Logger log = LoggerFactory.getLogger(WatdivBenchmark.class);

    static HashMap<String, Long> nbResultsPerQuery = new HashMap<>();

    @Param("sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_10084.sparql")
    public String a_query;

    @Param({EngineTypes.TDB, EngineTypes.Sage})
    public String b_engine;

    @Param("target/watdiv10M")
    public String z_dbPath;


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
        try {
            ec.query = Files.readString(Paths.get(a_query), StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.debug("{}", ec.query);
    }

    @Benchmark
    public long execute(SetupBenchmark.BenchmarkExecutionContext ec) throws Exception {
        Pair<Long, Long> nbResultsAndPreempt = SetupBenchmark.execute(ec, b_engine);

        log.debug("Got {} results for this query in {} pause/resume.", nbResultsAndPreempt.left, nbResultsAndPreempt.right);

        // (TODO) remove this from the benchmarked part
        if (nbResultsPerQuery.containsKey(a_query)) {
            long previousNbResults = nbResultsPerQuery.get(a_query);
            if (previousNbResults != nbResultsAndPreempt.left) {
                throw (new Exception(String.format("/!\\ not the same number of results on %s: %s vs %s.",
                        a_query, previousNbResults, nbResultsAndPreempt.left)));
            }
        } else {
            nbResultsPerQuery.put(a_query, nbResultsAndPreempt.left);
        }

        return nbResultsAndPreempt.left;
    }

    /**
     * Run the benchmark on Watdiv.
     * @param args [0] The path to the DB directory (default: "target").
     */
    public static void main(String[] args) throws RunnerException {
        Optional<String> dirPath_opt = (args.length > 0) ? Optional.of(args[0]) : Optional.empty();

        Watdiv10M watdiv = new Watdiv10M(Optional.of("datasets")); // creates the db if need be

        // create all the runners' options
        List<Options> options = createOptions(watdiv, List.of(QueryTypes.Long),
                // EngineTypes.TDB
                // EngineTypes.Sage,
                // EngineTypes.TDBForceOrder,
                // EngineTypes.SageForceOrder,
                // EngineTypes.SageForceOrderTimeout1ms
                //EngineTypes.SageForceOrderTimeout1s,
                // EngineTypes.SageForceOrderTimeout30s
                EngineTypes.SageForceOrderTimeout60s
        );

        // testing only one query
        // options = customsOptions(watdiv, "sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_10020.sparql",
        //        EngineTypes.SageForceOrderTimeout1ms);
                // EngineTypes.TDB);*/

        for (Options opt : options) {
            new Runner(opt).run();
        }

    }

    /**
     * Creates a list of options to run the benchmarks. It divides the benchmark into
     * multiples runs, starting from short to long queries, and for each kind of query,
     * every engine set. Each individual run is saved in its respective file at the end
     * of each benchmark.
     */
    public static List<Options> createOptions(Watdiv10M watdiv, List<String> queryTypes, String... engines) {
        ArrayList<Options> options = new ArrayList<>();
        if (queryTypes.contains(QueryTypes.Short)) {
            for (String engine : engines) // run all shorts
                options.add(runShort(watdiv, engine));
        }

        if (queryTypes.contains(QueryTypes.Medium)) {
            for (String engine : engines) // then run all mediums
                options.add(runMedium(watdiv, engine));
        }

        if (queryTypes.contains(QueryTypes.Long)) { // finally run all longs
            for (String engine : engines)
                options.add(runLong(watdiv, engine));
        }
        return options.stream().filter(Objects::nonNull).toList();
    }

    /**
     * Mostly for debugging purpose. Instead of running categories of queries, it runs a specific
     * query individually, and do not export the data.
     */
    public static List<Options> customsOptions(Watdiv10M watdiv, String query, String... engines) {
        ArrayList<Options> options = new ArrayList<>();
        for (String engine : engines) {
            options.add(runCommon(watdiv, List.of(query), engine)
                    .warmupIterations(3)
                    .forks(100)
                    .mode(Mode.SingleShotTime)
                    .timeout(TimeValue.seconds(10000))
                    //.jvmArgsAppend("-XX:-TieredCompilation", "-XX:-BackgroundCompilation")
                    // Such option comes from an issue with `jmh` where identical run, ie forks would
                    // yield twice increased/decreased execution time due to different JVM compiler choices.
                    // see: <https://stackoverflow.com/questions/32047440/different-benchmarking-results-between-forks-in-jmh>
                    .jvmArgsAppend("-XX:-BackgroundCompilation")
                    //.jvmArgsAppend("-XX:-BackgroundCompilation -XX:+UnlockDiagnosticVMOptions -XX:+PrintCompilation -verbose:gc")
                    .build());
        }
        return options;
    }

    // some interesting remark about microbenchmarking at https://wiki.openjdk.org/display/HotSpot/MicroBenchmarks
    public static ChainedOptionsBuilder runCommon(Watdiv10M watdiv, List<String> queries, String engine) {
        String[] queriesAsArray = queries.toArray(String[]::new);
        return new OptionsBuilder()
                .include(WatdivBenchmark.class.getSimpleName())
                .param("z_dbPath", watdiv.dbPath_asStr)
                .param("a_query", queriesAsArray)
                .param("b_engine", engine)
                .jvmArgsAppend("-XX:-BackgroundCompilation")
                .forks(1)
                .threads(1)
                .resultFormat(ResultFormatType.CSV);
    }

    public static Options runShort(Watdiv10M watdiv, String engine) {
        Path outfile = Path.of(String.format("sage-jena-benchmarks/results/%s-%s-Short.csv",
                WatdivBenchmark.class.getSimpleName(),
                engine));

        if (fileExistsAndNotEmpty(outfile)) return null;

        return runCommon(watdiv, watdiv.shortQueries, engine)
                .warmupIterations(2) // 2 warmups
                .warmupTime(TimeValue.seconds(5)) // 5s per warmup
                .measurementIterations(1) // averaged over 10s
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .result(outfile.toString())
                .build();
    }

    public static Options runMedium(Watdiv10M watdiv, String engine) {
        Path outfile = Path.of(String.format("sage-jena-benchmarks/results/%s-%s-Medium.csv",
                WatdivBenchmark.class.getSimpleName(),
                engine));

        if (fileExistsAndNotEmpty(outfile)) return null;

        return runCommon(watdiv, watdiv.mediumQueries, engine)
                .warmupIterations(5)
                .forks(2)
                .mode(Mode.SingleShotTime)
                .result(outfile.toString())
                .build();
    }

    public static Options runLong(Watdiv10M watdiv, String engine) {
        Path outfile = Path.of(String.format("sage-jena-benchmarks/results/%s-%s-Long.csv",
                WatdivBenchmark.class.getSimpleName(),
                engine));

        if (fileExistsAndNotEmpty(outfile)) return null;

        return runCommon(watdiv, watdiv.longQueries, engine)
                .warmupIterations(2)
                .forks(1)
                .mode(Mode.SingleShotTime)
                .result(outfile.toString())
                .build();
    }



    public static boolean fileExistsAndNotEmpty(Path path) {
        if (path.toFile().exists()) {
            List<List<String>> records = new ArrayList<>();
            try (Scanner scanner = new Scanner(path.toFile());) {
                while (scanner.hasNextLine()) {
                    records.add(getRecordFromLine(scanner.nextLine()));
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            return !records.isEmpty();
        }
        return false; // file does not exist
    }

    private static List<String> getRecordFromLine(String line) {
        List<String> values = new ArrayList<>();
        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter(",");
            while (rowScanner.hasNext()) {
                values.add(rowScanner.next());
            }
        }
        return values;
    }

}
