package fr.univnantes.gdd.fedup.sourceselection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FedUPSourceSelectionPerformerTest {

    @Test
    public void is_stopping_condition_ok_on_exact_match() {
        List<Map<String, String>> ss1 = new ArrayList<>();
        Map m1 = new HashMap();
        m1.put("?g1", "@s1");
        m1.put("?g2", "@s2");
        ss1.add(m1);

        List<Map<String, String>> opti = new ArrayList<>();
        Map m2 = new HashMap();
        m2.put("?g1", "@s1");
        m2.put("?g2", "@s2");
        opti.add(m2);

        int nbMissing = FedUPSourceSelectionPerformer.countMissingAssignments(ss1, opti);
        assertEquals(0, nbMissing);
    }

    @Test
    public void is_stopping_condition_ok_on_ss_strictly_included_in_opti() {
        List<Map<String, String>> ss1 = new ArrayList<>();
        Map m1 = new HashMap();
        m1.put("?g1", "@s1");
        m1.put("?g2", "@s2");
        // ?g2 optional here implies ?g1 also executed alone because of FedX dependency
        ss1.add(m1);

        List<Map<String, String>> opti = new ArrayList<>();
        Map m2 = new HashMap();
        m2.put("?g1", "@s1");
        // because ?g2 is optional
        opti.add(m2);

        int nbMissing = FedUPSourceSelectionPerformer.countMissingAssignments(ss1, opti);
        assertEquals(0, nbMissing);
    }

    @Test
    public void is_stopping_condition_ok_with_missing_assignments() {
        List<Map<String, String>> ss1 = new ArrayList<>();
        Map m1 = new HashMap();
        m1.put("?g1", "@s1");
        m1.put("?g2", "@s2");
        // ?g2 optional here implies ?g1 also executed alone because of FedX dependency
        ss1.add(m1);

        List<Map<String, String>> opti = new ArrayList<>();
        Map m2 = new HashMap();
        m2.put("?g1", "@s1");
        Map m3 = new HashMap();
        m3.put("?g1", "@s2");
        // because ?g2 is optional
        opti.add(m2);
        opti.add(m3);

        int nbMissing = FedUPSourceSelectionPerformer.countMissingAssignments(ss1, opti);
        assertEquals(1, nbMissing);
    }

    /* *************************************************************** */

    @Test
    public void does_inclusion_remove_inclusions() {
        List<Map<String, String>> sources = new ArrayList<>();

        Map m = new HashMap();
        m.put("?g1", "@s1");

        Map m2 = new HashMap();
        m2.put("?g1", "@s1");
        m2.put("?g2", "@s2");


        sources.add(m);
        sources.add(m2);

        var result = FedUPSourceSelectionPerformer.removeInclusions(sources);
        assertEquals(1, result.size());
        assertEquals(m2, result.get(0)); // m included in m2 so the former is removed
    }

    @Test
    public void does_inclusion_remove_duplicates() {
        List<Map<String, String>> sources = new ArrayList<>();

        Map m = new HashMap();
        m.put("?g1", "@s1");

        Map m2 = new HashMap();
        m2.put("?g1", "@s1");


        sources.add(m);
        sources.add(m2);

        var result = FedUPSourceSelectionPerformer.removeInclusions(sources);
        assertEquals(1, result.size());
        assertEquals(m, result.get(0));
    }

}