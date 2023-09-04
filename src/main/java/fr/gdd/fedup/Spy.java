package fr.gdd.fedup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Spy {
    
    public static enum Status {OK, TIMEOUT, ERROR}

    public Status status = Spy.Status.OK;
    public long sourceSelectionTime = 0;
    public long executionTime = 0;
    public int numASKQueries = 0;
    public int numSolutions = 0;
    public int numAssignments = 0;
    public int numFoundAssignments = 0;
    public int numValidAssignments = 0;
    public long tpwss = 0;
    public List<Map<String, String>> assignments = null;
    public Map<String, String> tpAliases = new HashMap<>();
    public List<String> solutions = null;
}
