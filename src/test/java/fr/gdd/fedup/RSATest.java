package fr.gdd.fedup;

import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.summary.SummaryFactory;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.commons.io.FileUtils;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Try to create RSA using the identity data of FedShop.
 * TODO create summaries of other kinds, not only Apache Jena !
 * TODO since we create SPARQL queries anyway…
 */
public class RSATest {

    private static final Logger log = LoggerFactory.getLogger(RSATest.class);

    private static final FedUP fedup =
            new FedUP(SummaryFactory.createIdentity(Location.create("./temp/fedup-id")),
                    IntStream.range(0, 100).boxed() // So it does not call getGraph that is slow (~2minutes process)
                            .flatMap(i -> Stream.of(String.format("http://www.vendor%s.fr/", i),
                                    String.format("http://www.ratingsite%s.fr/", i))).collect(Collectors.toSet())
                    )
            .shouldNotFactorize()
            .modifyEndpoints(e-> "http://localhost:5555/sparql?default-graph-uri="+(e.substring(0,e.length()-1)));
    // private static FedUP fedup;

    public static final String Q07F_RSA = """
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rev: <http://purl.org/stuff/rev#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
                        
            SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2 WHERE {
              VALUES ( ?bgp1 ?bgp2 ?bgp3 ) {
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> )
                ( <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> )
                (<http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=nan> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> ) }
                SERVICE ?bgp1 {
                    ?localProduct rdf:type bsbm:Product .
                    # const bsbm:Product72080
                    ?localProduct owl:sameAs bsbm:Product72080 .
                    ?localProduct rdfs:label ?productLabel .
                }
                OPTIONAL {
                    SERVICE ?bgp2 {
                        ?offer bsbm:product ?offerProduct .
                        ?offerProduct  owl:sameAs bsbm:Product72080 .
                        ?offer bsbm:price ?price .
                        ?offer bsbm:vendor ?vendor .
                        ?vendor rdfs:label ?vendorTitle .
                        ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#FR> .
                        ?offer bsbm:validTo ?date .
                        
                        # const "2008-04-25T00:00:00"^^xsd:dateTime < ?date
                        FILTER (?date > "2008-04-25T00:00:00"^^xsd:dateTime )
                    }
                }
                OPTIONAL {
                    SERVICE ?bgp3 {
                        ?review bsbm:reviewFor ?reviewProduct .
                        ?reviewProduct owl:sameAs bsbm:Product72080 .
                        ?review rev:reviewer ?reviewer .
                        ?reviewer foaf:name ?revName .
                        ?review dc:title ?revTitle .
                        OPTIONAL { ?review bsbm:rating1 ?rating1 . }
                        OPTIONAL { ?review bsbm:rating2 ?rating2 . }
                    }
                }
            }
            """;

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
    public void create_jena_rsa_of_q07f() {
        String rsa = fedup.query(FedShopTest.Q07F);
        log.debug(rsa);
    }

    @Disabled
    @Test
    public void run_factorized_rsa_of_q07f() {
        FedShopTest.measuredExecuteWithJena(Q07F_FEDUP_RSA_FACTORIZED);
    }

    @Disabled
    @Test
    void run_and_compare_all_fedshop_rsa_vs_fedup_rsa () throws IOException {
        Path fedupRSAFolderPath = Path.of("./queries/fedshop200-RSA-fedup");
        if (!fedupRSAFolderPath.toFile().isDirectory()) {
            log.info("Created the directory {}…", fedupRSAFolderPath.toAbsolutePath());
            Files.createDirectories(fedupRSAFolderPath);
        }
        Path fedshopQueriesPath = Path.of("./queries/fedshop");
        File[] queryFiles = fedshopQueriesPath.toFile().listFiles();
        assert queryFiles != null;
        for (File queryFile : queryFiles) {
            Path newRSAPath =  fedupRSAFolderPath.resolve(queryFile.getName());
            if (newRSAPath.toFile().exists()) {
                log.info("Skipping {}…", queryFile.getName());
                continue;
            }
            if (queryFile.getName().contains("q06")) {
                log.info("Skipping {}… (too long [source selection])", queryFile.getName());
                continue;
            }

            log.info("Reading {}…", queryFile.getName());
            String query = FileUtils.readFileToString(queryFile, "UTF-8");

            MultiSet<Binding> resultsOfFedUP = null;

            String fedupRSA = fedup.query(query);
            // fedup.shouldFactorize(); // TODO
            if (queryFile.getName().contains("q05")) { // should be executed with FedX
                // log.info("Skipping {}… (too long [execution time])", queryFile.getName());
                // continue;
                log.info("Reexecuting source selection for FedX…");
                TupleExpr fedupRSAFedx = fedup.queryToFedX(query); // reexecute all… TODO not to
                log.info("Executing with FedX…");
                resultsOfFedUP = FedShopTest.executeWithFedxWithBypassParser(fedupRSAFedx);
            } else {
                log.info("Executing with Jena…");
                resultsOfFedUP = FedShopTest.executeWithJena(fedupRSA);
            }

            Path fedshopRSAPath = Path.of("./queries/fedshop200-RSA/", queryFile.getName());
            String rsa = FileUtils.readFileToString(fedshopRSAPath.toFile(), "UTF-8")
                    .replace("http://localhost:8890/", "http://localhost:5555/"); // Virtuoso's address
            log.debug("Read RSA: {}", rsa);
            MultiSet<Binding> resultsOfRSA = FedShopTest.executeWithJena(rsa);

            // comparing key
            resultsOfRSA = reorderMultiset(resultsOfRSA);
            resultsOfFedUP = reorderMultiset(resultsOfFedUP);

            /*assert (resultsOfRSA.uniqueSet().containsAll(resultsOfFedUP.uniqueSet()));
                    assert (resultsOfFedUP.uniqueSet().containsAll(resultsOfRSA.uniqueSet()));*/

            assertEquals(resultsOfRSA.uniqueSet().stream().map(Binding::toString).sorted(String::compareTo).toList(),
                    resultsOfFedUP.uniqueSet().stream().map(Binding::toString).sorted(String::compareTo).toList());
            log.debug("Got {} distinct results.", resultsOfFedUP.uniqueSet().size());

            // comparing the number of entries
            assertEquals(resultsOfRSA.stream().map(Binding::toString).sorted().toList(),
                    resultsOfFedUP.stream().map(Binding::toString).sorted().toList());
            log.debug("Got {} total results.", resultsOfFedUP.size());

            Files.writeString(newRSAPath, fedupRSA);
        }
    }

    public static MultiSet<Binding> reorderMultiset(MultiSet<Binding> bindings) {
        MultiSet<Binding> result = new HashMultiSet<>();
        List<Binding> reordered = bindings.stream().map(b -> {
                    Iterator<Var> vars = b.vars();
                    List<Var> sortedVars = new ArrayList<>();
                    while (vars.hasNext()) {
                        sortedVars.add(vars.next());
                    }
                    sortedVars = sortedVars.stream().map(Var::getVarName).sorted().map(Var::alloc).toList();

                    BindingBuilder bindingBuilder = BindingFactory.builder();
                    sortedVars.forEach(v -> bindingBuilder.add(v, b.get(v)));
                    return bindingBuilder.build();
                }
            ).toList();
        result.addAll(reordered);
        return result;
    }

    @Disabled
    @Test
    public void execute_FedShop_s_RSA_with_apache_jena () {
        FedShopTest.PRINTRESULTTHRESHOLD = 1000;
        FedShopTest.measuredExecuteWithJena(RSATest.Q07F_FEDUP_RSA);
        FedShopTest.measuredExecuteWithJena(RSATest.Q07F_RSA);
        VirtuosoTest.executeOnVirtuoso(RSATest.Q07F_FEDUP_RSA);
        // Virtuoso does not like SERVICE ?variable, however, FedShop built its RSA using this… Therefore:
        // Virtuoso 37000 Error SP031: SPARQL compiler: Internal error: Usupported combination of subqueries and service invocations
        VirtuosoTest.executeOnVirtuoso(RSATest.Q07F_RSA);
    }


    public static String Q07F_FEDUP_RSA_FACTORIZED =  """
                SELECT  ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2
                WHERE
                  { VALUES ?_vv_1 { <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> }
                    SERVICE SILENT ?_vv_1
                      { ?localProduct
                                  a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                        ?localProduct
                                  <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                        ?localProduct
                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                      }
                    OPTIONAL
                      {   { VALUES ?_v_1 { <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr> }
                            SERVICE SILENT ?_v_1
                              { ?reviewProduct
                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .\s
                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .\s
                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .\s
                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .\s
                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                OPTIONAL
                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                              }
                          }
                        UNION
                          { VALUES ?_v_2 { <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr> <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr> }
                            SERVICE SILENT ?_v_2
                              { ?reviewProduct
                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .\s
                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .\s
                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .\s
                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .\s
                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                OPTIONAL
                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                OPTIONAL
                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                              }
                          }
                      }
                  }
                """;

    public static String Q07F_FEDUP_RSA = """
            SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2
            WHERE
              {   {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                          { ?localProduct
                                                      a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .
                                            ?localProduct
                                                      <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .
                                            ?localProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                          }
                                        OPTIONAL
                                          {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                                                                  { ?reviewProduct
                                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                    OPTIONAL
                                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                                    OPTIONAL
                                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                                  }
                                                              }
                                                            UNION
                                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                                                  { ?reviewProduct
                                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                    OPTIONAL
                                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                                  }
                                                              }
                                                          }
                                                        UNION
                                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                                              { ?reviewProduct
                                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                OPTIONAL
                                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                              }
                                                          }
                                                      }
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                                                          { ?reviewProduct
                                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                          }
                                                      }
                                                  }
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                                      { ?reviewProduct
                                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                      }
                                                  }
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                                                  { ?reviewProduct
                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                  }
                                              }
                                          }
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                          { ?localProduct
                                                      a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .
                                            ?localProduct
                                                      <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .
                                            ?localProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                          }
                                        OPTIONAL
                                          {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                                                                  { ?reviewProduct
                                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                    OPTIONAL
                                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                                    OPTIONAL
                                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                                  }
                                                              }
                                                            UNION
                                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                                                  { ?reviewProduct
                                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                    OPTIONAL
                                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                                  }
                                                              }
                                                          }
                                                        UNION
                                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                                              { ?reviewProduct
                                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                OPTIONAL
                                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                              }
                                                          }
                                                      }
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                                                          { ?reviewProduct
                                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                          }
                                                      }
                                                  }
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                                      { ?reviewProduct
                                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                      }
                                                  }
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                                                  { ?reviewProduct
                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                  }
                                              }
                                          }
                                      }
                                  }
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr>
                                      { ?localProduct
                                                  a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .
                                        ?localProduct
                                                  <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .
                                        ?localProduct
                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                      }
                                    OPTIONAL
                                      {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                                                              { ?reviewProduct
                                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                OPTIONAL
                                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                                OPTIONAL
                                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                              }
                                                          }
                                                        UNION
                                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                                              { ?reviewProduct
                                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                                OPTIONAL
                                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                              }
                                                          }
                                                      }
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                                          { ?reviewProduct
                                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                          }
                                                      }
                                                  }
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                                                      { ?reviewProduct
                                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                      }
                                                  }
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                                  { ?reviewProduct
                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                  }
                                              }
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                                              { ?reviewProduct
                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                              }
                                          }
                                      }
                                  }
                              }
                            UNION
                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                  { ?localProduct
                                              a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .
                                    ?localProduct
                                              <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .
                                    ?localProduct
                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                  }
                                OPTIONAL
                                  {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                                                          { ?reviewProduct
                                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                          }
                                                      }
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                                          { ?reviewProduct
                                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                            OPTIONAL
                                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                          }
                                                      }
                                                  }
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                                      { ?reviewProduct
                                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                      }
                                                  }
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                                                  { ?reviewProduct
                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                  }
                                              }
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                              { ?reviewProduct
                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                              }
                                          }
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                                          { ?reviewProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                            OPTIONAL
                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                          }
                                      }
                                  }
                              }
                          }
                        UNION
                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                              { ?localProduct
                                          a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .
                                ?localProduct
                                          <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .
                                ?localProduct
                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                              }
                            OPTIONAL
                              {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                                                      { ?reviewProduct
                                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                      }
                                                  }
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                                      { ?reviewProduct
                                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                        OPTIONAL
                                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                      }
                                                  }
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                                  { ?reviewProduct
                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                  }
                                              }
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                                              { ?reviewProduct
                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                              }
                                          }
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                          { ?reviewProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                            OPTIONAL
                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                          }
                                      }
                                  }
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                                      { ?reviewProduct
                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                        OPTIONAL
                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                      }
                                  }
                              }
                          }
                      }
                    UNION
                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                          { ?localProduct
                                      a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .
                            ?localProduct
                                      <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .
                            ?localProduct
                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                          }
                        OPTIONAL
                          {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                                                  { ?reviewProduct
                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                  }
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                                  { ?reviewProduct
                                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                    OPTIONAL
                                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                                  }
                                              }
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                              { ?reviewProduct
                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                              }
                                          }
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                                          { ?reviewProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                            OPTIONAL
                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                            OPTIONAL
                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                          }
                                      }
                                  }
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                      { ?reviewProduct
                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                        OPTIONAL
                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                      }
                                  }
                              }
                            UNION
                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                                  { ?reviewProduct
                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                    OPTIONAL
                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                  }
                              }
                          }
                      }
                  }
                UNION
                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                      { ?localProduct
                                  a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .
                        ?localProduct
                                  <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .
                        ?localProduct
                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                      }
                    OPTIONAL
                      {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                                              { ?reviewProduct
                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                              }
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                              { ?reviewProduct
                                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                                OPTIONAL
                                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                              }
                                          }
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                          { ?reviewProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                            ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                            ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                            ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                            ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                            OPTIONAL
                                              { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                          }
                                      }
                                  }
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                                      { ?reviewProduct
                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                        ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                        ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                        ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                        ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                        OPTIONAL
                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1>  ?rating1 }
                                        OPTIONAL
                                          { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                      }
                                  }
                              }
                            UNION
                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                  { ?reviewProduct
                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                    ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                    ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                    ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                    ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                    OPTIONAL
                                      { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                                  }
                              }
                          }
                        UNION
                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                              { ?reviewProduct
                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080> .
                                ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor>  ?reviewProduct .
                                ?review  <http://purl.org/stuff/rev#reviewer>  ?reviewer .
                                ?reviewer  <http://xmlns.com/foaf/0.1/name>  ?revName .
                                ?review  <http://purl.org/dc/elements/1.1/title>  ?revTitle
                                OPTIONAL
                                  { ?review  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2>  ?rating2 }
                              }
                          }
                      }
                  }
              }
            """;

}
