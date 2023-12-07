package fr.gdd.fedup;

import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQueryMore;
import org.apache.jena.sparql.engine.binding.Binding;
import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.federated.repository.FedXRepositoryConnection;
import org.eclipse.rdf4j.federated.structures.FedXTupleQuery;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.repository.sail.SailTupleQuery;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Simple test by running on FedShop.
 *
 *         Have a virtuoso endpoint running with fedshop200…
 *         Virtuoso Open Source Edition (Column Store) (multi threaded)
 *         Version 7.2.6.3233-pthreads as of Jun 22 2021 (111d17e5b)
 *         Compiled for Mac OS 11 (Apple Silicon) (aarch64-apple-darwin20.5.0)
 *
 *         Listening on localhost:5555
 */
public class FedShopTest {

    private static final Logger log = LoggerFactory.getLogger(FedShopTest.class);
    public static Integer PRINTRESULTTHRESHOLD = 10;

    public static FedUP fedup = new FedUP(new Summary(new ModuloOnSuffix(1),
            Location.create("./temp/fedup-h0" )))
            .modifyEndpoints(e-> "http://localhost:5555/sparql?default-graph-uri="+(e.substring(0,e.length()-1)));

    FedXRepository fedx = FedXFactory.newFederation()
            .withConfig(new FedXConfig() // same as FedUP-experiment
                    .withBoundJoinBlockSize(20) // 10+10 or 20+20 ?
                    .withJoinWorkerThreads(20)
                    .withUnionWorkerThreads(20)
                    .withDebugQueryPlan(false))
            .withSparqlEndpoints(List.of()).create();


    public final static String Q05J = """
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                                
                SELECT DISTINCT ?product ?localProductLabel WHERE {
                    ?localProduct rdfs:label ?localProductLabel .
                                
                    ?localProduct bsbm:productFeature ?localProdFeature .
                    ?localProduct bsbm:productPropertyNumeric1 ?simProperty1 .
                    ?localProduct bsbm:productPropertyNumeric2 ?simProperty2 .
                                
                    ?localProduct owl:sameAs ?product .
                    ?localProdFeature owl:sameAs ?prodFeature .
                                
                    ?localProductXYZ bsbm:productFeature ?localProdFeatureXYZ .
                    ?localProductXYZ bsbm:productPropertyNumeric1 ?origProperty1 .
                    ?localProductXYZ bsbm:productPropertyNumeric2 ?origProperty2 .
                                
                    # const bsbm:Product171547
                    ?localProductXYZ owl:sameAs bsbm:Product171547 .
                    ?localProdFeatureXYZ owl:sameAs ?prodFeature .
                                
                    FILTER(bsbm:Product171547 != ?product)
                    # Values are pre-determined because we knew the boundaries from the normal distribution
                    FILTER(?simProperty1 < (?origProperty1 + 20) && ?simProperty1 > (?origProperty1 - 20))
                    FILTER(?simProperty2 < (?origProperty2 + 70) && ?simProperty2 > (?origProperty2 - 70))
                }
                ORDER BY ?product ?localProductLabel
                LIMIT 5
                """;

    public final static String Q07F = """
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rev: <http://purl.org/stuff/rev#>
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
                PREFIX dc: <http://purl.org/dc/elements/1.1/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                                
                SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2 WHERE {
                  ?localProduct owl:sameAs bsbm:Product72080 .
                  ?localProduct rdf:type bsbm:Product .
                  ?localProduct rdfs:label ?productLabel .
                  OPTIONAL {
                    ?offerProduct  owl:sameAs bsbm:Product72080 .
                    ?offer bsbm:product ?offerProduct .
                    ?offer bsbm:price ?price .
                    ?offer bsbm:vendor ?vendor .
                    ?vendor rdfs:label ?vendorTitle .
                    ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#FR> .
                    ?offer bsbm:validTo ?date .
                    FILTER (?date > "2008-04-25T00:00:00"^^xsd:dateTime )
                  }
                  OPTIONAL {
                    ?reviewProduct owl:sameAs bsbm:Product72080 .
                    ?review bsbm:reviewFor ?reviewProduct .
                    ?review rev:reviewer ?reviewer .
                    ?reviewer foaf:name ?revName .
                    ?review dc:title ?revTitle .
                    OPTIONAL { ?review bsbm:rating1 ?rating1 . }
                    OPTIONAL { ?review bsbm:rating2 ?rating2 . }
                  }
                }
             """;

    public final static String Q10E = """
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
                        
            SELECT DISTINCT ?offer ?price WHERE {
                # const bsbm:Product87739
                ?offer bsbm:product ?localProductXYZ .
                ?localProductXYZ owl:sameAs bsbm:Product87739 .
                ?offer bsbm:vendor ?vendor .
                #?offer dc:publisher ?vendor .
                ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
                ?offer bsbm:deliveryDays ?deliveryDays .
                FILTER(?deliveryDays <= 3)
                ?offer bsbm:price ?price .
                ?offer bsbm:validTo ?date .
                # const "2008-04-10T00:00:00"^^xsd:dateTime < ?date
                FILTER (?date > "2008-04-10T00:00:00"^^xsd:dateTime )
            }
            ORDER BY ?offer ?price
            LIMIT 10
            """;

    public final static String Q11A = """
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
                        
            SELECT DISTINCT ?property ?hasValue ?isValueOf WHERE {
                # const <http://www.vendor6.fr/Offer886>
                <http://www.vendor6.fr/Offer886> bsbm:product ?product .
                { <http://www.vendor6.fr/Offer886> ?property ?hasValue }
                UNION
                { ?isValueOf ?property <http://www.vendor6.fr/Offer886> }
            }
            """;

    /* ********************************************************************** */

    // (TODO) conditional run
    @Disabled
    @Test
    public void small_try_to_benchmark_fedshop_on_multiple_executors() {
        doItAll(Q07F);
    }

    @Disabled
    @Test
    public void run_all_izmir_fedshop200() throws IOException {
        List<ImmutablePair<String, String>> queries = Files.walk(Paths.get("./queries/izmir_fedshop/"))
                .filter(p-> p.toString().endsWith(".sparql"))
                .map(p -> {
                    try {
                        return new ImmutablePair<>(p.getFileName().toString(), new String(Files.readAllBytes(p)));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).toList();
        // #1 get all queries from folder
        queries.forEach(q -> {
            log.info("Started {}…", q.getLeft());
            doItAllWithByPassAndPrint(q.getRight(), q.getLeft());
        });
    }

    @Disabled
    @Test
    public void run_on_q05j() {
        doItAllWithByPassAndPrint(Q11A, "Q");
    }


    /* **************************************************************** */

    public void doItAll(String query) {
        Pair<String, Long> serviceQuery = measuredSourceSelection(query);
        measuredExecuteWithFedX(serviceQuery.getLeft());  // start with fedx since expected to be quicker
        measuredExecuteWithJena(serviceQuery.getLeft());
    }

    public void doItAllAndPrint(String query, String shortname) {
        Pair<String, Long> serviceQuery = measuredSourceSelection(query);
        long fedxTime = measuredExecuteWithFedX(serviceQuery.getLeft()); // start with fedx since expected to be quicker
        long jenaTime = measuredExecuteWithJena(serviceQuery.getLeft());
        System.out.println(String.format("%s %s %s %s", shortname, serviceQuery.getRight(), jenaTime, fedxTime));
    }

    public Pair<String, Long> measuredSourceSelection(String originalQuery) {
        long current = System.currentTimeMillis();
        String result = fedup.query(originalQuery);
        long elapsed = System.currentTimeMillis() - current;
        log.info("FedUP took {} ms to perform source selection.", elapsed);
        return new ImmutablePair<>(result, elapsed);
    }

    public static long measuredExecuteWithJena(String serviceQuery) {
        long elapsed = -1;
        MultiSet<Binding> serviceResults = new HashMultiSet<>();
        try (QueryExecution qe =  QueryExecutionFactory.create(serviceQuery, DatasetFactory.empty())) {
            long current = System.currentTimeMillis();
            ResultSet results = qe.execSelect();
            while (results.hasNext()) {
                serviceResults.add(results.nextBinding());
            }
            elapsed = System.currentTimeMillis() - current;
            log.info("Jena took {} ms to get {} results.", elapsed, serviceResults.size());
        }

        if (serviceResults.size() <= PRINTRESULTTHRESHOLD) {
            log.debug("Results:\n{}", String.join("\n", serviceResults.entrySet().stream().map(Object::toString).toList()));
        }
        return elapsed;
    }

    public long measuredExecuteWithFedX(String serviceQuery) {
        MultiSet<BindingSet> serviceResults = new HashMultiSet<>();

        long elapsed = -1;


        try (FedXRepositoryConnection conn = fedx.getConnection()) {
            long current = System.currentTimeMillis();
            // parsing may take a long long time…
            TupleQuery tq = conn.prepareTupleQuery(serviceQuery);
            try (TupleQueryResult tqRes = tq.evaluate()) {
                while (tqRes.hasNext()) {
                    serviceResults.add(tqRes.next());
                }
            }
            elapsed = System.currentTimeMillis() - current;
            log.info("FedX took {} ms to get {} results.", elapsed, serviceResults.size());
        }

        if (serviceResults.size() <= PRINTRESULTTHRESHOLD) {
            log.debug("Results:\n{}", String.join("\n", serviceResults.entrySet().stream().map(Object::toString).toList()));
        }
        return elapsed;
    }

    public void doItAllWithByPassAndPrint (String query, String shortname) {
        long current = System.currentTimeMillis();
        TupleExpr toExecute = fedup.queryToFedX(query);
        long ssFedX = System.currentTimeMillis() - current;
        long fedxTime = measuredExecuteWithFedXWithBypassParser(toExecute);

        current = System.currentTimeMillis();
        // Op toExecuteJena = fedup.queryToJena(query);
        long ssJena = -1; // System.currentTimeMillis() - current;
        long jenaTime = -1;
        //long jenaTime = measuredExecuteWithJenaWithBypassParser(toExecuteJena);

        System.out.printf("%s %s %s %s %s%n", shortname, ssFedX, ssJena, jenaTime, fedxTime);
    }

    public long measuredExecuteWithFedXWithBypassParser(TupleExpr fedXExpr) {
        MultiSet<BindingSet> serviceResults = new HashMultiSet<>();

        long elapsed = -1;

        try (FedXRepositoryConnection conn = fedx.getConnection()) {
            long current = System.currentTimeMillis();
            TupleQuery tq = new FedXTupleQuery(new SailTupleQuery(new ParsedTupleQuery("", fedXExpr), conn));
            try (TupleQueryResult tqRes = tq.evaluate()) {
                while (tqRes.hasNext()) {
                    serviceResults.add(tqRes.next());
                }
            }
            elapsed = System.currentTimeMillis() - current;
            log.info("FedX took {} ms to get {} results.", elapsed, serviceResults.size());
        }

        if (serviceResults.size() <= PRINTRESULTTHRESHOLD) {
            log.debug("Results:\n{}", String.join("\n", serviceResults.entrySet().stream().map(Object::toString).toList()));
        }

        return elapsed;
    }

    public long measuredExecuteWithJenaWithBypassParser(Op serviceQuery) {
        long elapsed = -1;
        MultiSet<Binding> serviceResults = new HashMultiSet<>();

        Query q = OpAsQueryMore.asQuery(serviceQuery);
        try (QueryExecution qe =  QueryExecutionFactory.create(q, DatasetFactory.empty())) {
            long current = System.currentTimeMillis();
            ResultSet results = qe.execSelect();
            while (results.hasNext()) {
                serviceResults.add(results.nextBinding());
            }
            elapsed = System.currentTimeMillis() - current;
            log.info("Jena took {} ms to get {} results.", elapsed, serviceResults.size());
        }

        if (serviceResults.size() <= PRINTRESULTTHRESHOLD) {
            log.debug("Results:\n{}", String.join("\n", serviceResults.entrySet().stream().map(Object::toString).toList()));
        }
        return elapsed;
    }


}
