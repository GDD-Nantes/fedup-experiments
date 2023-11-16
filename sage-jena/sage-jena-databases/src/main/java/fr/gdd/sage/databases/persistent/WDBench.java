package fr.gdd.sage.databases.persistent;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Open a `WDBench` dataset or create it when need be. The link
 * to the benchmark is <a href="https://github.com/MillenniumDB/WDBench">here</a>.
 * The downloaded file weights 8.52 GB.
 */
public class WDBench extends BenchmarkDataset {
    static Logger log = LoggerFactory.getLogger(WDBench.class);

    public static final String ARCHIVE_NAME = "truthy_direct_properties.tar.bz2";
    public static final String DB_NAME = "WDBench";
    public static final String EXTRACT_PATH = "extracted_truthy_direct_properties";
    public static final String DEFAULT_DB_PATH = "target";
    public static final String DOWNLOAD_URL = "https://figshare.com/ndownloader/files/34816081?private_link=50b7544ad6b1f51de060";

    public static final List<String> WHITELIST = List.of("truthy_direct_properties.nt");

    // even with LIMIT 100k, they fail
    public static final List<String> BLACKLIST = List.of("query_480.sparql", "query_51.sparql", "query_152.sparql");

    public WDBench(Optional<String> dbPath_opt) {
        super(dbPath_opt, DEFAULT_DB_PATH, DB_NAME, ARCHIVE_NAME, EXTRACT_PATH, DOWNLOAD_URL, WHITELIST, BLACKLIST);
        log.info("The download requires 8.52 GB.");
        log.info("Decompressing requires 146 GB.");
        log.info("The final database requires 162 GB.");
        log.warn("The ingestion may take a while. Consider ingesting on a dedicated machine using Jena's binary tdb2.xloader with parallel bulk loading, then download the database afterwards.");
        try {
            create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setQueries(String pathToQueries) throws IOException {
        super.setQueries(pathToQueries);
        /* if (Path.of("sage-jena-benchmarks/results/wdbench_opts.csv").toFile().exists()) {
            Reader reader = new FileReader("sage-jena-benchmarks/results/wdbench_opts.csv");
            String f = IOUtils.toString(reader);
            String[] splitted = f.split(",|\n");
            List<String> skiplist = Arrays.stream(splitted).toList().stream().filter(s -> s.contains(".sparql")).collect(Collectors.toList());
            log.debug("Skipping {} queries:", skiplist.size());
            for (String skip : skiplist) {
                log.debug("\t {}", skip);
            }
            queries.removeAll(skiplist);
        } */
    }
}
