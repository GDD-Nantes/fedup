package fr.gdd.fedup.sourceselection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.query.algebra.StatementPattern;

import com.fluidops.fedx.algebra.StatementSource;

public class SourceAssignments {
    
    private static SourceAssignments instance = null;

    private List<Map<StatementPattern, List<StatementSource>>> assignments = new ArrayList<>();
    private int index = 0;

    private SourceAssignments() { }

    public static SourceAssignments getInstance() {
        if (instance == null) {
            instance = new SourceAssignments();
        }
        return instance;
    }

    public boolean hasNextAssignment() {
        return this.index < this.assignments.size();
    }

    public synchronized Map<StatementPattern, List<StatementSource>> getNextAssignment() {
        this.index += 1;
        return this.assignments.get(index - 1);
    }

    public List<Map<StatementPattern, List<StatementSource>>> getAssignments() {
        return this.assignments;
    }

    public void setAssignments(List<Map<StatementPattern, List<StatementSource>>> assignments) {
        this.assignments = assignments;
        this.index = 0;
    }
}
