package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.InMemoryInstanceOfTDB2WithSimpleData;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.jena.JenaBackend;
import fr.gdd.sage.jena.SerializableRecord;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The issue 14 is quite difficult to reproduce. It happens rarely, and only
 * with timeout. To help reproduce, increasing the number of pause/resume by
 * setting it low, i.e. 1 millisecond helps.
 *
 * This test set aims to create all combinations on a simple hand-made dataset
 * in order to reproduce the issue, then fix it…
 * Most importantly, it tests with combination that are not on `limit` threshold.
 */
public class ChasingIssue14Test {

    static Dataset dataset = null;
    static JenaBackend backend = null;

    static NodeId any = null;
    static NodeId named = null;
    static NodeId owns = null;
    static NodeId bob = null;
    static NodeId alice = null;
    static NodeId cat = null;


    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2WithSimpleData().getDataset();
        backend = new JenaBackend(dataset);

        any = backend.any();
        owns  = backend.getId("<http://owns>", SPOC.PREDICATE);
        named = backend.getId("<http://named>", SPOC.PREDICATE);

        bob = backend.getId("<http://Bob>", SPOC.OBJECT);
        alice = backend.getId("<http://Alice>", SPOC.OBJECT);
        cat = backend.getId("<http://cat>");
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    @Test
    public void has_next_does_not_move_the_cursor() {
        // important to figure out since some backend move their cursor on
        // `hasNext` and `next` is only to produce the value.
        BackendIterator<NodeId, SerializableRecord> it1 = backend.search(any, named, any);
        for (int i = 0; i < 100; ++i) { // only has 2 items yet can call hasNext 100 times
            assert (it1.hasNext());
        }
        it1.next();
        it1.next();
        for (int i = 0; i < 100; ++i) {
            assertFalse(it1.hasNext());
        }
    }


    @Test
    public void stopping_at_each_step_of_2_triples_with_singleton_and_null() {
        // SELECT * WHERE { ?person <named> ?name . ?name <owns> ?animal }
        BackendIterator<NodeId, SerializableRecord> it1 = backend.search(any, named, any);

        // even at very first we stop… (despite in normal run, would not be possible)
        SerializableRecord saved_it1 = it1.current();
        it1 = backend.search(any, named, any);
        it1.skip(saved_it1);
        it1.next();
        assertEquals(bob, it1.getId(SPOC.OBJECT));

        // also save at very first it2 before its `next`
        BackendIterator<NodeId, SerializableRecord> it2 = backend.search(bob, owns, any);
        saved_it1 = it1.previous();
        SerializableRecord saved_it2 = it2.current();
        it1 = backend.search(any, named, any);
        it1.skip(saved_it1);
        it2 = backend.search(bob, owns, any);
        it2.skip(saved_it2);
        it1.next();
        assertEquals(bob, it1.getId(SPOC.OBJECT));
        assertFalse(it2.hasNext());

        assert(it1.hasNext());
        saved_it1 = it1.current();
        it1 = backend.search(any, named, any);
        it1.skip(saved_it1);
        it1.next();
        assertEquals(alice, it1.getId(SPOC.OBJECT));

        it2 = backend.search(alice, owns, any);
        saved_it2 = it2.current();
        saved_it1 = it1.previous();
        it1 = backend.search(any, named, any);
        it1.skip(saved_it1);
        it1.next();
        assertEquals(alice, it1.getId(SPOC.OBJECT));
        it2 = backend.search(alice, owns, any);
        it2.skip(saved_it2);
        assert(it2.hasNext());
        it2.next();
        assertEquals(cat, it2.getId(SPOC.OBJECT));
        assertFalse(it2.hasNext());

        saved_it1 = it1.previous();
        saved_it2 = it2.current();
        it1 = backend.search(any, named, any);
        it1.skip(saved_it1);
        it1.next();
        assertEquals(alice, it1.getId(SPOC.OBJECT));
        it2 = backend.search(alice, owns, any);
        it2.skip(saved_it2);
        assertFalse(it2.hasNext());

        assertFalse(it1.hasNext());
        saved_it1 = it1.current();
        it1 = backend.search(any, named, any);
        it1.skip(saved_it1);
        assertFalse(it1.hasNext());
    }



}
