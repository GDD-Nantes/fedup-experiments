package fr.gdd.sage.datasets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

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

    public static final List<String> BLACKLIST = List.of(); // on query

    public WDBench(Optional<String> dbPath_opt) {
        super(dbPath_opt, DEFAULT_DB_PATH, DB_NAME, ARCHIVE_NAME, EXTRACT_PATH, DOWNLOAD_URL, WHITELIST, BLACKLIST);
        log.info("The download requires 8.52 GB.");
        log.info("Decompressing requires 146 GB.");
        log.info("The final database requires 110 GB.");
        log.warn("The ingestion may take a while. Consider ingesting on a dedicated machine using Jena's binary tdb2.xloader with parallel bulk loading, then download the database afterwards.");
        try {
            create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
