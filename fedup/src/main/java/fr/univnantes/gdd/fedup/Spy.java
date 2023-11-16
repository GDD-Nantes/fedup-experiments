package fr.univnantes.gdd.fedup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Spy {

    private static Spy instance = null;
    
    public enum Status {OK, TIMEOUT, ERROR}

    public Status status = Spy.Status.OK;
    public long sourceSelectionTime = 0;
    public long executionTime = 0;
    public long runtime = 0; // total execution time including the time to release resources
    public int numASKQueries = 0;
    public int numSolutions = 0;
    public int numAssignments = 0;
    public long tpwss = 0;
    public List<Map<String, String>> assignments = null;
    public Map<String, String> tpAliases = new HashMap<>();
    public List<String> solutions = new ArrayList<>();

    public static Spy getInstance() {
        if (instance == null) {
            instance = new Spy();
        }
        return instance;
    }

    public static void reset() {
        instance = new Spy();
    }
}
