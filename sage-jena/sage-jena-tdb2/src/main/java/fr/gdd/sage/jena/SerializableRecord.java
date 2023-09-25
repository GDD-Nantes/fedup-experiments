package fr.gdd.sage.jena;

import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.dboe.base.record.Record;

import java.io.*;

/**
 * Jena Records must be serializable to be shared easily, for instance
 * through the network.
 **/
public class SerializableRecord implements Serializable {

    /**
     * Record that targets a position in Jena's BPlusTree.
     */
    private Record record;

    /**
     * The number of elements between the beginning and the targeted record.
     */
    private long offset;

    /* ***************************************************************************** */

    public SerializableRecord(Record record, long offset) {
        this.record = record;
        this.offset = offset;
    }

    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        // write the `Long` before, thus we can conveniently call `readAllBytes` at
        // deserialization time.
        out.writeLong(offset);
        out.write(record.getKey());
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.offset = in.readLong();
        byte[] key = in.readAllBytes();
        record = new Record(key, new byte[0]);
    }

    public Record getRecord() {
        return record;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {

        return String.format("%s (%sth)", Bytes.asHex(record.getKey()), offset);
    }
}
