package fr.gdd.sage.jena;

import fr.gdd.sage.InMemoryInstanceOfTDB2;
import org.apache.jena.query.Dataset;
import org.apache.jena.shared.NotFoundException;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Testing the few functions provided by {@link JenaBackend}. Name of tests are self-explanatory.
 **/
public class JenaBackendTest {
    static Dataset dataset = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2().getDataset();
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    @Test
    public void simple_initialization_with_jena_backend_then_retrieve_id_back_and_forth() {
        JenaBackend backend = new JenaBackend(dataset);

        NodeId city0Id = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City0>");
        String city0 = backend.getValue(city0Id);
        assertEquals("http://db.uwaterloo.ca/~galuc/wsdbm/City0", city0);
    }

    @Test
    public void node_id_does_not_exist() {
        JenaBackend backend = new JenaBackend(dataset);

        Exception exception = assertThrows(NotFoundException.class, () -> {
            NodeId city0Id = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/CityUnkown>");
        });
    }

    @Test
    public void retrieve_the_node_id_of_a_graph() {
        JenaBackend backend = new JenaBackend(dataset);

        NodeId graphAId = backend.getId("<https://graphA.org>");
        String graphA = backend.getValue(graphAId);
        assertEquals("https://graphA.org", graphA);
    }
}