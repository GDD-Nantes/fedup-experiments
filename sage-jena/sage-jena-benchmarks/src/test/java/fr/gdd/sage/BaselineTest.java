package fr.gdd.sage;

import fr.gdd.sage.databases.persistent.Watdiv10M;
import org.apache.jena.tdb2.TDB2Factory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BaselineTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "WATDIV", matches = "true")
    public void simple_execution_of_few_queries_saved_in_file () throws IOException {
        Watdiv10M watdiv10M = new Watdiv10M(Optional.of("../datasets/"));
        List<String> queries = List.of(
                "../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_0.sparql",
                "../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_100.sparql",
                "../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_1000.sparql");

        Path outputFilePath = Path.of("./target/watdiv_test.csv");

        Files.deleteIfExists(outputFilePath);
        assertFalse(outputFilePath.toFile().exists());

        Map<String, String> query2ActualQuery = queries.stream().collect(
                Collectors.toMap(String::toString, // key: query file path
                (path) -> { // actual query
                    try {
                        String query = String.join("", Files.readAllLines(Path.of(path)));
                        return query;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));

        Baseline baseline = new Baseline(watdiv10M.dbPath_asStr, query2ActualQuery, 1,
                outputFilePath.toString());

        baseline.execute();
        assertTrue(outputFilePath.toFile().exists());

        Baseline baseline2 = new Baseline(null, null, 1, outputFilePath.toString());
        baseline2.loadCSV();

        assertTrue(0 <= baseline2.executionTime("../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_0.sparql"));
        assertTrue(0 <= baseline2.executionTime("../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_100.sparql"));
        assertTrue(0 <= baseline2.executionTime("../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_1000.sparql"));

        assertEquals(326, baseline2.nbResults("../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_0.sparql"));
        assertEquals(0, baseline2.nbResults("../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_100.sparql"));
        assertEquals(123513, baseline2.nbResults("../sage-jena-benchmarks/queries/watdiv_with_sage_plan/query_1000.sparql"));

        Files.deleteIfExists(outputFilePath);
    }

}