package fr.gdd.sage;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * Create an in memory database following TDB2 model. Useful for testing.
 **/
public class InMemoryInstanceOfTDB2ForOptional {

    Dataset dataset;

    public InMemoryInstanceOfTDB2ForOptional() {
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        // Model containing the 10 first triples of Dataset Watdiv.10M
        // Careful, the order in the DB is not identical to that of the array
        List<String> statements = Arrays.asList(
                    "<http://Alice> <http://address> <http://nantes> .",
                    "<http://Bob>   <http://address> <http://paris>  .",
                    "<http://Carol> <http://address> <http://nantes> .",
                    "<http://Alice> <http://own>     <http://cat> .",
                    "<http://Alice> <http://own>     <http://dog> .",
                    "<http://Alice> <http://own>     <http://snake> ."
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());
        dataset.setDefaultModel(model);
    }

    public Dataset getDataset() {
        return dataset;
    }

}