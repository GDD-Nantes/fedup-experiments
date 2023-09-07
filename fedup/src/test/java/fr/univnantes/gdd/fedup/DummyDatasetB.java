package fr.univnantes.gdd.fedup;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class DummyDatasetB {
    Model dataset;

    public DummyDatasetB() throws IOException {
        // Model containing the 10 first triples of Dataset Watdiv.10M
        // Careful, the order in the DB is not identical to that of the array
        List<String> statements = Arrays.asList(
                "<http://B> <http://person> <http://named> <http://Carol>.",
                "<http://B> <http://person> <http://named> <http://David>.",
                "<http://B> <http://Carol>  <http://owns>  <http://cat>.",
                "<http://B> <http://Carol>  <http://nb_animals> 12."
        );
        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        dataset = Rio.parse(statementsStream, "", RDFFormat.NQUADS);
    }

    public Model getDataset() {
        return dataset;
    }
}
