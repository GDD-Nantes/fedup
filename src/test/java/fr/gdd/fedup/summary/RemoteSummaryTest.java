package fr.gdd.fedup.summary;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class RemoteSummaryTest {

    private final static Logger log = LoggerFactory.getLogger(RemoteSummaryTest.class);
    private static final String VIRTUOSO_ENDPOINT_URL = "http://localhost:5555/sparql";

    @Disabled
    @Test
    public void try_out_an_identity_remote_virtuoso() {
        final Summary s = new Summary(new TransformCopy());
        s.setRemote(VIRTUOSO_ENDPOINT_URL);
        Set<String> graphs = s.getGraphs();
        log.debug("Got {} graphs.", graphs.size());
        // virtuoso creates 6 additional graphs by default, we don't care much.
        // as they won't appear in the results of source selection anyway.
        log.debug(graphs.toString());

        Op query = Algebra.compile(QueryFactory.create("""
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                SELECT (COUNT(*) as ?count) WHERE { ?s owl:sameAs ?o }
                """));

        log.debug(s.querySummary(query).toString());
    }

}
