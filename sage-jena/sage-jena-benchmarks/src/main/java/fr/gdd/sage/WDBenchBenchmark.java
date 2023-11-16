package fr.gdd.sage;

import fr.gdd.sage.databases.persistent.BenchmarkDataset;
import fr.gdd.sage.databases.persistent.WDBench;
import fr.gdd.sage.generics.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.jena.dboe.base.file.FileException;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb2.TDB2Factory;
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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@State(Scope.Benchmark)
public class WDBenchBenchmark {

    final static Logger log = LoggerFactory.getLogger(WDBenchBenchmark.class);
    static String BASELINE_FILE = "sage-jena-benchmarks/results/wdbench_opts_baseline.csv";

    @Param("sage-jena-benchmarks/queries/wdbench_opts/query_99.sparql")
    public String a_query;

    @Param({EngineTypes.TDB, EngineTypes.Sage})
    public String b_engine;

    @Param("sage-jena-benchmarks/target/WDBench")
    public String z_dbPath;

    public static Baseline baseline;


    @Setup(Level.Trial)
    public void setup(SetupBenchmark.BenchmarkExecutionContext ec) {
        try {
            baseline = new Baseline(null, null, null, BASELINE_FILE);
            baseline.loadCSV();
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
            // The paper enforces a limit on the number of results. Of course, execution is much faster.
            ec.query += "LIMIT 100000";
        } catch (Exception e) {
            e.printStackTrace();
        }
        log.debug("{}", ec.query);
    }

    @Benchmark
    public long execute(SetupBenchmark.BenchmarkExecutionContext ec) throws Exception {
        Pair<Long, Long> nbResultsAndPreempt = SetupBenchmark.execute(ec, b_engine);
        log.debug("Got {} results for this query in {} pause/resume.", nbResultsAndPreempt.left, nbResultsAndPreempt.right);

        if (!baseline.query2Results.get(a_query).equals(nbResultsAndPreempt.left)) {
            throw (new Exception(String.format("/!\\ not the same number of results on %s: %s vs %s.",
                    a_query, baseline.query2Results.get(a_query), nbResultsAndPreempt.left)));
        }

        return nbResultsAndPreempt.left;
    }

    /**
     * Run the benchmark on WDBench
     * @param args [0] The path to the DB directory (default: "target").
     */
    public static void main(String[] args) throws RunnerException, IOException {
        Optional<String> dirPath_opt = (args.length > 0) ? Optional.of(args[0]) : Optional.empty();

        WDBench wdbench = new WDBench(Optional.of("datasets"));
        wdbench.setQueries("sage-jena-benchmarks/queries/wdbench_opts_with_sage_plan/");

        List<String> queries = wdbench.getQueries();
        Map<String, String> query2ActualQuery = queries.stream().collect(
                Collectors.toMap(String::toString, // key: query file path
                        (path) -> { // actual query
                            try {
                                String query = String.join(" ", Files.readAllLines(Path.of(path)));
                                return query + " LIMIT 100000";
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }));
        Baseline baselineWDBench = new Baseline(wdbench.dbPath_asStr, query2ActualQuery,
                3, "sage-jena-benchmarks/results/wdbench_opts_baseline.csv");

        baselineWDBench.execute();
        // (TODO) should fully release dataset, yet cannot manage to do it properly… The process seems
        //  to hang on the locking… Once the baseline is processed, for now, you need to restart the benchmark…

        // wdbench.queries = wdbench.queries.stream().filter(q -> baselineWDBench.query2Results.get(q) < 100000).collect(Collectors.toList());

        // create all the runners' options
        List<Options> options = createOptions(wdbench, List.of(QueryTypes.Long),
                // EngineTypes.TDB
                // EngineTypes.Sage,
                // EngineTypes.TDBForceOrder
                // EngineTypes.SageForceOrder,
                EngineTypes.SageForceOrderTimeout1ms
                // EngineTypes.SageForceOrderTimeout1s,
                // EngineTypes.SageForceOrderTimeout30s
                //EngineTypes.SageForceOrderTimeout60s);
        );

        // testing only one query
        // options = customsOptions(wdbench.dbPath_asStr, "sage-jena-benchmarks/queries/wdbench_opts_with_sage_plan/query_266.sparql",
        //      EngineTypes.SageForceOrderTimeout1ms);

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
    public static List<Options> createOptions(BenchmarkDataset benched, List<String> queryTypes, String... engines) {
        ArrayList<Options> options = new ArrayList<>();
        if (queryTypes.contains(QueryTypes.Short)) {
            for (String engine : engines) // run all shorts
                options.add(runShort(benched, engine));
        }

        if (queryTypes.contains(QueryTypes.Medium)) {
            for (String engine : engines) // then run all mediums
                options.add(runMedium(benched, engine));
        }

        if (queryTypes.contains(QueryTypes.Long)) { // finally run all longs
            for (String engine : engines)
                options.add(runLong(benched, engine));
        }
        return options.stream().filter(Objects::nonNull).toList();
    }

    /**
     * Mostly for debugging purpose. Instead of running categories of queries, it runs a specific
     * query individually, and do not export the data.
     */
    public static List<Options> customsOptions(String pathToDataset, String query, String... engines) {
        ArrayList<Options> options = new ArrayList<>();
        for (String engine : engines) {
            options.add(runCommon(pathToDataset, List.of(query), engine)
                    .warmupIterations(3)
                    .forks(1)
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
    public static ChainedOptionsBuilder runCommon(String pathToDataset, List<String> queries, String engine) {
        String[] queriesAsArray = queries.toArray(String[]::new);
        return new OptionsBuilder()
                .include(WDBenchBenchmark.class.getSimpleName())
                .param("z_dbPath", pathToDataset)
                .param("a_query", queriesAsArray)
                .param("b_engine", engine)
                .jvmArgsAppend("-XX:-BackgroundCompilation")
                .forks(1)
                .threads(1)
                .resultFormat(ResultFormatType.CSV);
    }

    public static Options runShort(BenchmarkDataset benched, String engine) {
        Path outfile = Path.of(String.format("sage-jena-benchmarks/results/%s-%s-Short.csv",
                benched.getClass().getSimpleName(),
                engine));

        if (fileExistsAndNotEmpty(outfile)) return null;

        return runCommon(benched.dbPath_asStr, benched.getQueries(), engine)
                .warmupIterations(2) // 2 warmups
                .warmupTime(TimeValue.seconds(5)) // 5s per warmup
                .measurementIterations(1) // averaged over 10s
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .result(outfile.toString())
                .build();
    }

    public static Options runMedium(BenchmarkDataset benched, String engine) {
        Path outfile = Path.of(String.format("sage-jena-benchmarks/results/%s-%s-Medium.csv",
                benched.getClass().getSimpleName(),
                engine));

        if (fileExistsAndNotEmpty(outfile)) return null;

        return runCommon(benched.dbPath_asStr, benched.getQueries(), engine)
                .warmupIterations(5)
                .forks(2)
                .mode(Mode.SingleShotTime)
                .result(outfile.toString())
                .build();
    }

    public static Options runLong(BenchmarkDataset benched, String engine) {
        Path outfile = Path.of(String.format("sage-jena-benchmarks/results/%s-%s-Long.csv",
                benched.getClass().getSimpleName(),
                engine));

        if (fileExistsAndNotEmpty(outfile)) return null;

        return runCommon(benched.dbPath_asStr, benched.getQueries(), engine)
                .warmupIterations(2)
                .forks(1)
                .mode(Mode.SingleShotTime)
                .result(outfile.toString())
                .build();
    }

    /* ******************************************************************* */

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
