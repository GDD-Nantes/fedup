package fr.gdd.fedup.sourceselection;

import java.util.List;
import java.util.Map;

import org.aksw.simba.quetsal.core.HibiscusSourceSelection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.algebra.StatementSource.StatementSourceType;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.cache.Cache.StatementSourceAssurance;
import com.fluidops.fedx.structures.Endpoint;
import com.fluidops.fedx.structures.QueryInfo;
import com.fluidops.fedx.structures.QueryType;
import com.fluidops.fedx.structures.SubQuery;

import fr.univnantes.gdd.fedup.Spy;
import fr.univnantes.gdd.fedup.Utils;

public class HibiscusSourceSelectionPerformer extends SourceSelectionPerformer {

    private static Logger logger = LogManager.getLogger(HibiscusSourceSelectionPerformer.class);

    public HibiscusSourceSelectionPerformer(SailRepositoryConnection connection) {
        super(connection);
    }
    
    @Override
    public List<Map<StatementPattern, List<StatementSource>>> performSourceSelection(String queryString, List<Map<String, String>> groundtruth, Spy spy) throws Exception {
        logger.info("Source selection computed using HiBiSCuS");
        
        List<Endpoint> endpoints = connection.getEndpoints();
        Cache cache = connection.getFederation().getCache();
        QueryInfo queryInfo = new QueryInfo(connection, queryString, QueryType.SELECT, null);

        cache.clear();

        SourceSelection sourceSelection = new SourceSelection(endpoints, cache, queryInfo, spy);

        long startTime = System.currentTimeMillis();
        sourceSelection.performSourceSelection(Utils.getBasicGraphPatterns(queryString));
        long endTime = System.currentTimeMillis();

        spy.sourceSelectionTime = endTime - startTime;

        return List.of(sourceSelection.getStmtToSources());
    }

    private class SourceSelection extends HibiscusSourceSelection {

        private Spy spy;

        public SourceSelection(List<Endpoint> endpoints, Cache cache, QueryInfo queryInfo, Spy spy) {
            super(endpoints, cache, queryInfo);
            this.spy = spy;
        }

        @Override
        public void performSourceSelection(List<List<StatementPattern>> bgpGroups) {
            super.performSourceSelection(bgpGroups);
        }
        
        @Override
        public void cache_ASKselection(StatementPattern stmt) {
            SubQuery subQuery = new SubQuery(stmt);
            for (Endpoint endpoint: endpoints) {
                StatementSourceAssurance assurance = cache.canProvideStatements(subQuery, endpoint);
                if (assurance == StatementSourceAssurance.HAS_LOCAL_STATEMENTS) {
                    addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.LOCAL));
                } else if (assurance == StatementSourceAssurance.HAS_REMOTE_STATEMENTS) {
                    addSource(stmt, new StatementSource(endpoint.getId(), StatementSourceType.REMOTE));
                } else if (assurance == StatementSourceAssurance.POSSIBLY_HAS_STATEMENTS) {
                    remoteCheckTasks.add(new CheckTaskPair(endpoint, stmt));
                    spy.numASKQueries += 1;
                }
            }
        }
    }
}
