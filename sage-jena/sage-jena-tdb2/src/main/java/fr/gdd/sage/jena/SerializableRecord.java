package fr.gdd.sage.jena;

import org.apache.jena.dboe.base.record.Record;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;



/**
 * Jena Records must be serializable to be shared easily, for instance
 * through the network.
 **/
public class SerializableRecord implements Serializable {

    public Record record;

    public SerializableRecord(Record record) {
        this.record = record;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.write(record.getKey());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        byte[] key = in.readAllBytes();
        record = new Record(key, new byte[0]);
    }
    
}
