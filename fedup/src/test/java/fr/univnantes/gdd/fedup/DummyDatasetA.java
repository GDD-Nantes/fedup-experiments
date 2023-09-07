package fr.univnantes.gdd.fedup;

import org.eclipse.rdf4j.http.client.HttpClientSessionManager;
import org.eclipse.rdf4j.http.client.SharedHttpClientSessionManager;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class DummyDatasetA {

    Model dataset;
    SailRepository repository;

    public DummyDatasetA () throws IOException {
        // Model containing the 10 first triples of Dataset Watdiv.10M
        // Careful, the order in the DB is not identical to that of the array
        List<String> statements = Arrays.asList(
                "<http://A> <http://person> <http://named> <http://Alice>.",
                "<http://A> <http://person> <http://named> <http://Bob>.",
                "<http://A> <http://Alice>  <http://owns>  <http://cat>."
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        dataset = Rio.parse(statementsStream, "", RDFFormat.NQUADS);

        repository = new SailRepository(new MemoryStore());


        HttpClientSessionManager manager = new SharedHttpClientSessionManager();
        manager.createSPARQLProtocolSession("meow", "woof");


        repository.setHttpClientSessionManager(manager);

        repository.initialize();
        try (SailRepositoryConnection conn = repository.getConnection()) {
            conn.add(statementsStream, "", RDFFormat.NQUADS);
        }



    }

    public Model getDataset() {
        return dataset;
    }

    public Repository getRepository() {
        return repository;
    }
}
