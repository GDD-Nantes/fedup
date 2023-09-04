package fr.gdd.fedup.sourceselection;

import java.util.List;
import java.util.Map;

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import com.fluidops.fedx.FedXConnection;
import com.fluidops.fedx.algebra.StatementSource;

import fr.univnantes.gdd.fedup.Spy;

public abstract class SourceSelectionPerformer {
    
    protected FedXConnection connection;

    public SourceSelectionPerformer(SailRepositoryConnection connection) {
        this.connection = ((FedXConnection) connection.getSailConnection());
    }

    public abstract List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(String queryString, List<Map<String, String>> groundtruth, Spy spy) throws Exception;

}
