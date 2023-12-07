package fr.gdd.fedup;

import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.summary.SummaryFactory;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Try to create RSA using the identity data of FedShop.
 * TODO create summaries of other kinds, not only Apache Jena !
 * TODO since we create SPARQL queries anyway…
 */
public class RSATest {

    private static final Logger log = LoggerFactory.getLogger(RSATest.class);

    @Disabled
    @Test
    public void try_remote_identity_summary_to_create_RSA() {
        Summary summary = new Summary(new TransformCopy()).setRemote("http://localhost:5555/sparql");

        // unfortunately, jena does not handle GRAPH clauses in SERVICE, which precludes
        // the use of remote
        FedUP fedup = new FedUP(summary)
                .modifyEndpoints(e-> "http://localhost:5555/sparql?default-graph-uri=" + e);

        String rsa = fedup.query(FedShopTest.Q11A);

        log.debug(rsa);
    }

    @Disabled
    @Test
    public void try_local_jena_identity() {
        Summary summary = SummaryFactory.createIdentity(Location.create("./temp/fedup-id"));
        FedUP fedup = new FedUP(summary)
                .modifyEndpoints(e-> "http://localhost:5555/sparql?default-graph-uri="+(e.substring(0,e.length()-1)));
        String rsa = fedup.query(FedShopTest.Q07F);

        log.debug(rsa);
    }


}
