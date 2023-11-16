package fr.gdd.sage.databases.inmemory;

import org.apache.jena.dboe.sys.SystemIndex;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.SystemTDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class SmallBlocksInMemoryTDB2ForCardinality {
    Logger log = LoggerFactory.getLogger(SmallBlocksInMemoryTDB2ForCardinality.class);

    Dataset dataset;

    public SmallBlocksInMemoryTDB2ForCardinality() throws NoSuchFieldException, IllegalAccessException {
        dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        // 42 elements per node at current times
        log.debug("There are {} elements per node in the BTree.", SystemIndex.BlockSizeTest / SystemTDB.LenIndexTripleRecord);
        // to create depth 2, I need 42 element at root, each having 42 elements (1764)

        List<String> statements = new ArrayList<>();
        for (int i = 0; i < 50; ++i) {
            statements.add(String.format("<http://Alice> <http://own> <http://animal_%s> .", i));
            for (int j= 0; j< 50; ++j) {
                statements.add(String.format("<http://animal_%s> <http://originates> <http://region_%s> .", i, j));
            }
        }

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model model = ModelFactory.createDefaultModel();
        model.read(statementsStream, "", Lang.NT.getLabel());
        dataset.setDefaultModel(model);
    }

    public Dataset getDataset() {
        return dataset;
    }
}
