package fr.gdd.sage.datasets;

import fr.gdd.sage.generics.Pair;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Open `watdiv` dataset or create (ie. download, unarchive, then ingest) it when
 * need be.
 *
 * (TODO) abstract this class so others can be built on the same pattern.
 */
public class Watdiv10M extends BenchmarkDataset {
    static Logger log = LoggerFactory.getLogger(Watdiv10M.class);

    public static final String ARCHIVE_NAME = "watdiv.10M.tar.bz2";
    public static final String DB_NAME = "watdiv10M";
    public static final String EXTRACT_PATH = "extracted_watdiv.10M";
    public static final String DEFAULT_DB_PATH = "target";
    public static final String DOWNLOAD_URL = "https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2";

    public static final String QUERIES_PATH = "sage-jena-benchmarks/queries/watdiv_with_sage_plan";

    public static final List<String> whitelist = List.of("watdiv.10M.nt");
    // fails or too time-consuming because no limit
    public static final List<String> blacklist = List.of("query_10069.sparql", "query_10150.sparql", "query_10091.sparql");

    public final List<Pair<String, String>> queries;

    // above 100s
    final List<String> longQueryNames = List.of("query_10020.sparql", "query_10082.sparql", "query_10168.sparql",
            "query_10078.sparql", "query_10083.sparql");
    public List<String> longQueries = new ArrayList<>();
    // between 1s to 100s
    final List<String> mediumQueryNames = List.of("query_10122.sparql", "query_10012.sparql", "query_10061.sparql");
    public List<String> mediumQueries = new ArrayList<>();
    // the rest below 1s

    public List<String> shortQueries = new ArrayList<>();

    public Watdiv10M(Optional<String> dbPath_opt) {
        super(dbPath_opt, DEFAULT_DB_PATH, DB_NAME, ARCHIVE_NAME, EXTRACT_PATH, DOWNLOAD_URL, whitelist, blacklist);
        log.info("The download requires 56 MB.");
        log.info("Decompressing requires 1.4 GB.");
        log.info("Database requires 1.4 GB.");
        try {
            create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        log.info("Reading queries…");
        this.queries = getQueries(QUERIES_PATH, blacklist);
        categorizeQueries(queries);
    }

    /**
     * Divide the performance analysis into 3 categories to ease benchmarking.
     */
    public void categorizeQueries(List<Pair<String, String>> queries) {
        longQueries = new ArrayList<>();
        mediumQueries = new ArrayList<>();
        shortQueries = new ArrayList<>();
        for (String queryName : queries.stream().map((p) -> p.left).toList()) {
            if (!longQueryNames.stream().filter(e -> queryName.contains(e)).toList().isEmpty()) {
                longQueries.add(queryName);
            } else if (!mediumQueryNames.stream().filter(e -> queryName.contains(e)).toList().isEmpty()) {
                mediumQueries.add(queryName);
            } else {
                shortQueries.add(queryName);
            }
        }
    }

    /**
     * @return A list of pairs containing the name of the query and its actual content.
     */
    static public ArrayList<Pair<String,String>> getQueries(String queriesPath_asStr, List<String> blacklist) {
        ArrayList<Pair<String, String>> queries = new ArrayList<>();
        Path queriesPath = Paths.get(queriesPath_asStr);
        if (!queriesPath.toFile().exists()) {
            return queries; // no queries
        };

        File[] queryFiles = queriesPath.toFile().listFiles((dir, name) -> name.endsWith(".sparql"));

        log.info("Queries folder contains {} SPARQL queries.", queryFiles.length);
        for (File queryFile : queryFiles) {
            if (blacklist.contains(queryFile.getName())) { continue; }
            try {
                String query = Files.readString(queryFile.toPath(), StandardCharsets.UTF_8);
                query = query.replace('\n', ' '); // to get a clearer one line rendering
                query = query.replace('\t', ' ');
                // query = String.format("# %s\n%s", queryFile.getName(), query);
                queries.add(new Pair<>(queryFile.getPath(), query));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return queries;
    }

}
