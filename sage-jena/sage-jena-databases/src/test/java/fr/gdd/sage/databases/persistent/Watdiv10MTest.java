package fr.gdd.sage.databases.persistent;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Watdiv10MTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "WATDIV", matches = "true")
    public void download_extract_ingest_clean() {
        Path testingPath = Path.of("datasets/watdiv-test");
        try {
            FileUtils.deleteDirectory(testingPath.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        assertFalse(testingPath.toFile().exists()); // start the test anew

        Watdiv10M watdiv10M = new Watdiv10M(Optional.of(testingPath.toString()));
        assertTrue(testingPath.toFile().exists());
        assertTrue(new File(watdiv10M.dbPath_asStr).exists());
        assertFalse(watdiv10M.pathToArchive.toFile().exists());
        assertFalse(watdiv10M.fullExtractPath.toFile().exists());

        // cleanup test
        try {
            FileUtils.deleteDirectory(testingPath.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
