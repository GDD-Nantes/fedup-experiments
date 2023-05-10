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
public class InMemoryInstanceOfTDB2 {

    Dataset dataset;

    public InMemoryInstanceOfTDB2() {
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        // Model containing the 10 first triples of Dataset Watdiv.10M
        // Careful, the order in the DB is not identical to that of the array
        List<String> statements = Arrays.asList(
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City0>   <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City100> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City101> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City102> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country17> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City103> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country3> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City104> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City105> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country0> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City106> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country10> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City107> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country23> .",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City108> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country1>."
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());

        statements = Arrays.asList(
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City0>   <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country6>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City100> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2>.",
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City101> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country2> ."
        );
        statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelA = ModelFactory.createDefaultModel();
        modelA.read(statementsStream, "", Lang.NT.getLabel());

        statements = Arrays.asList(
                "<http://db.uwaterloo.ca/~galuc/wsdbm/City102> <http://www.geonames.org/ontology#parentCountry> <http://db.uwaterloo.ca/~galuc/wsdbm/Country17> ."
        );
        statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelB = ModelFactory.createDefaultModel();
        modelB.read(statementsStream, "", Lang.NT.getLabel());

        dataset.setDefaultModel(model);
        dataset.addNamedModel("https://graphA.org", modelA);
        dataset.addNamedModel("https://graphB.org", modelB);

        assertEquals(10, dataset.getDefaultModel().size());
        assertEquals(4, dataset.getUnionModel().size());
    }

    public Dataset getDataset() {
        return dataset;
    }

}