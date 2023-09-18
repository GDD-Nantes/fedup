package fr.gdd.fedup.source.selection;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.raw.RAWConstants;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingRoot;
import org.apache.jena.sparql.util.Context;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FedUPEngineTest {

    Logger log = LoggerFactory.getLogger(FedUPEngineTest.class);

    @Test
    public void testing_on_small_in_memory_summary() {
        Summary ims = InMemorySummaryFactory.getSimplePetsSummary();

        ims.getSummary().begin(ReadWrite.READ);
        Iterator<Quad> quads = ims.getSummary().asDatasetGraph().find();
        while (quads.hasNext()){
            log.debug("Summary has "+ quads.next());
        }
        ims.getSummary().end();

        Context context = ims.getSummary().getContext().copy() // those are dataset spec'
                .set(RAWConstants.limitRWs, 1000L) // LIMIT 1000
                .set(RAWConstants.timeout, 60000L); // 60s

        FedUPEngine fedup = new FedUPEngine();
        QueryIterator it = fedup.executeAsFederatedQuery("SELECT * WHERE {?p <http://auth/owns> ?a}",
                ims.getSummary().asDatasetGraph(), BindingRoot.create(), context);

        for (int i = 0; i < 1000; ++i) {
            assertTrue(it.hasNext());
            Binding b = it.next();
            log.debug("Found " + b);
        }

        assertFalse(it.hasNext());


    }

}