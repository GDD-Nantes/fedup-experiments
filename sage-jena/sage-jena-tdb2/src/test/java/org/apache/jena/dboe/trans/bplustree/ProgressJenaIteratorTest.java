package org.apache.jena.dboe.trans.bplustree;

import fr.gdd.sage.databases.inmemory.InMemoryInstanceOfTDB2;
import fr.gdd.sage.databases.persistent.Watdiv10M;
import fr.gdd.sage.generics.LazyIterator;
import fr.gdd.sage.generics.Pair;
import fr.gdd.sage.interfaces.BackendIterator;
import fr.gdd.sage.interfaces.SPOC;
import fr.gdd.sage.io.SageOutput;
import fr.gdd.sage.jena.JenaBackend;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb2.store.NodeId;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProgressJenaIteratorTest {

    Logger log = LoggerFactory.getLogger(ProgressJenaIteratorTest.class);

    static Dataset dataset = null;
    static JenaBackend backend = null;

    static NodeId predicate = null;
    static NodeId any = null;

    @BeforeAll
    public static void initializeDB() {
        dataset = new InMemoryInstanceOfTDB2().getDataset();

        backend = new JenaBackend(dataset);
        predicate = backend.getId("<http://www.geonames.org/ontology#parentCountry>");
        any = backend.any();
    }

    @AfterAll
    public static void closeDB() {
        dataset.abort();
        TDBInternal.expel(dataset.asDatasetGraph());
    }

    @Disabled
    @Test
    public void simple_progression_test_of_two_loops() {
        SageOutput output = run_loops(backend, new HashMap<>(), 1);
        // (TODO) (TODO) (TODO)
    }

    /**
     * Function that ease the creation of two loops, it stops at the `stopAt`^th next();
     */
    private static SageOutput run_loops(JenaBackend backend, HashMap<Integer, Serializable> savedState, int stopAt) {
        NodeId city_2 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/City102>");
        BackendIterator<NodeId, Serializable> it = backend.search(city_2, any, any);
        SageOutput<Serializable> output = new SageOutput<>();
        if (savedState.containsKey(0)) {
            it.skip(savedState.get(0));
        }

        int shouldStop = 0;
        while (it.hasNext()) {
            it.next();

            BackendIterator<?, Serializable> it_2 = backend.search(any, it.getId(SPOC.PREDICATE), any);
            if (savedState.containsKey(1)) {
                it_2.skip(savedState.get(1));
            }
            while (it_2.hasNext()) {
                it_2.next();

                shouldStop += 1;
                if (shouldStop >= stopAt) {
                    output.save(new Pair<>(0, it.previous()), new Pair<>(1, it_2.current()));
                    break;
                }
            }
            shouldStop += 1;
            if (shouldStop >= stopAt) {
                output.save(new Pair<>(0, it.current()));
                break;
            }
        }

        return output;
    }

    /* ********************************************************************************************* */
    // Since we don't know for sure the error on cardinality, we cannot set appropriate assertions


    @Disabled
    @Test
    public void cardinality_that_seems_not_good_watdiv() {
        // Comes from the observation that with 2k random walks, we
        // converge towards a cardinality that is not good
        // [main] DEBUG fr.gdd.sage.arq.SageOptimizer - triple ?v0 @http://xmlns.com/foaf/familyName ?v1 => 64861 elements
        // [main] DEBUG fr.gdd.sage.arq.SageOptimizer - triple ?v0 @http://xmlns.com/foaf/givenName ?v2 => 68338 elements
        new Watdiv10M(Optional.of("../target"));
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        NodeId family = backend.getId("<http://xmlns.com/foaf/familyName>");
        NodeId given = backend.getId("<http://xmlns.com/foaf/givenName>");

        PreemptJenaIterator.NB_WALKS = 200000;

        var it = backend.search(backend.any(), given, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(69970, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 69970, got {}.", casted.cardinality());

        it = backend.search(backend.any(), family, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        log.info("Expected 69970, got {}.", casted.cardinality());
        assertEquals(69970, casted.cardinality(Integer.MAX_VALUE));
    }


    @Disabled
    @Test
    public void cardinality_of_larger_triple_pattern_above_leaf_size_with_watdiv_with_query1000() {
        JenaBackend backend = new JenaBackend("../target/watdiv10M");
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country21>))"); // expect 2613 get 2613
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/validThrough> ?v3))"); // expect 36346 get 34100
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/includes> ?v1))"); // expect 90000 get 103616
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v1 <http://schema.org/text> ?v6))"); // expect 7476 get 7476
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://schema.org/eligibleQuantity> ?v4))"); // expect 90000 get 79454
        // OpBGP op = (OpBGP) SSE.parseOp("(bgp (?v0 <http://purl.org/goodrelations/price> ?v2))"); // expect 240000 get 234057

        NodeId price = backend.getId("<http://purl.org/goodrelations/price>");
        var it = backend.search(backend.any(), price, backend.any());
        ProgressJenaIterator casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(240000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 240000, got {}.", casted.cardinality(200000));

        NodeId eligible = backend.getId("<http://schema.org/eligibleQuantity>");
        it = backend.search(backend.any(), eligible, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(90000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 90000, got {}.", casted.cardinality(200000));

        NodeId text = backend.getId("<http://schema.org/text>");
        it = backend.search(backend.any(), text, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(7476, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 7476, got {}.", casted.cardinality(200000));

        NodeId include = backend.getId("<http://purl.org/goodrelations/includes>");
        it = backend.search(backend.any(), include, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(90000, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 90000, got {}.", casted.cardinality(200000));

        NodeId valid = backend.getId("<http://purl.org/goodrelations/validThrough>");
        it = backend.search(backend.any(), valid, backend.any());
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(36346, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 36346, got {}.", casted.cardinality(200000));

        NodeId region = backend.getId("<http://schema.org/eligibleRegion>");
        NodeId country21 = backend.getId("<http://db.uwaterloo.ca/~galuc/wsdbm/Country21>");
        it = backend.search(backend.any(), region, country21);
        casted = (ProgressJenaIterator) ((LazyIterator) it).iterator;
        assertEquals(2613, casted.cardinality(Integer.MAX_VALUE));
        log.info("Expected 2613, got {}.", casted.cardinality(200000));
    }

}