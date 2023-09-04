package fr.gdd.fedup.sourceselection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.rdf4j.query.algebra.StatementPattern;

import com.fluidops.fedx.algebra.EmptyStatementPattern;
import com.fluidops.fedx.algebra.ExclusiveStatement;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSourcePattern;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.optimizer.SourceSelection;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;

public class SourceSelectionInjector extends SourceSelection {
    
    public SourceSelectionInjector(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo) {
        super(endpoints, cache, queryInfo);
    }

    @Override
    public void performSourceSelection(List<List<StatementPattern>> bgps) { 
        SourceAssignments sourceAssignments = SourceAssignments.getInstance();
        Map<StatementPattern, List<StatementSource>> assignment = sourceAssignments.getNextAssignment();

        stmtToSources = new ConcurrentHashMap<StatementPattern, List<StatementSource>>();

        for (List<StatementPattern> bgp : bgps) {
            for (StatementPattern stmt : bgp) {
                stmtToSources.put(stmt, new ArrayList<StatementSource>());
                if (assignment.containsKey(stmt)) {
                    for (StatementSource source: assignment.get(stmt)) {
                        addSource(stmt, source);
                    }
                }
            }
        }

        for (StatementPattern stmt: stmtToSources.keySet()) {			
			List<StatementSource> sources = stmtToSources.get(stmt);
			if (sources.size() > 1) {
				StatementSourcePattern stmtNode = new StatementSourcePattern(stmt, this.queryInfo);
				for (StatementSource source: sources) {
                    stmtNode.addStatementSource(source);
                }
				stmt.replaceWith(stmtNode);
            } else if (sources.size() == 1) {
				stmt.replaceWith(new ExclusiveStatement(stmt, sources.get(0), this.queryInfo));
			} else {
				stmt.replaceWith(new EmptyStatementPattern(stmt));
			}
		}
    }
}
