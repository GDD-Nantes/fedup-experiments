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


/**
 * Create an in memory database following TDB2 model. This dataset is about creating the
 * simplest smallest dataset highlighting issues with `previous`, `next`, `NullIterator`,
 * `SingletonIterator`.
 **/
public class InMemoryInstanceOfTDB2WithSimpleData {

    Dataset dataset;

    public InMemoryInstanceOfTDB2WithSimpleData() {
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        // Model containing the 10 first triples of Dataset Watdiv.10M
        // Careful, the order in the DB is not identical to that of the array
        List<String> statements = Arrays.asList(
                "<http://person> <http://named> <http://Alice>.",
                "<http://person> <http://named> <http://Bob>.",
                "<http://Alice>  <http://owns>  <http://cat>."
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