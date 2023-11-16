package fr.gdd.sage.jena;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2;
import fr.gdd.sage.interfaces.BackendIterator;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.ext.xerces.impl.dv.util.Base64;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.Test;

import java.io.Serializable;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Single test to check if the serialization works properly. Will be useful
 * when sending the data through a network.
 * */
class SerializableRecordTest {

    @Test
    public void serialize_then_deserialize_a_record() {
        Dataset dataset = new InMemoryInstanceOfTDB2().getDataset();

        JenaBackend backend = new JenaBackend(dataset);
        NodeId predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        NodeId any = backend.any();

        BackendIterator<?, Serializable> it = backend.search(any, predicate, any);
        it.next();
        it.next();
        SerializableRecord sr = (SerializableRecord) it.current();
        assertEquals(2, sr.getOffset());

        byte[] serialized = SerializationUtils.serialize(sr);
        String encoded = Base64.encode(serialized);
        byte[] decoded = Base64.decode(encoded);
        SerializableRecord deserialized = SerializationUtils.deserialize(decoded);

        assertEquals(0, Bytes.compare(sr.getRecord().getKey(), deserialized.getRecord().getKey()));
        assertEquals(sr.getOffset(), deserialized.getOffset());

        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

}