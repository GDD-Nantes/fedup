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

    public static final String Q07F_RSA = """
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rev: <http://purl.org/stuff/rev#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
                        
            SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2 WHERE {\s
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
    public void try_local_jena_identity() {
        Summary summary = SummaryFactory.createIdentity(Location.create("./temp/fedup-id"));
        FedUP fedup = new FedUP(summary)
                .modifyEndpoints(e-> "http://localhost:5555/sparql?default-graph-uri="+(e.substring(0,e.length()-1)));
        String rsa = fedup.query(FedShopTest.Q07F);

        log.debug(rsa);
    }

    @Disabled
    @Test
    public void execute_FedShop_s_RSA_with_apache_jena () {
        FedShopTest.PRINTRESULTTHRESHOLD = 1000;
        FedShopTest.measuredExecuteWithJena(RSATest.Q07F_FEDUP_RSA);
        FedShopTest.measuredExecuteWithJena(RSATest.Q07F_RSA);
        VirtuosoTest.executeOnVirtuoso(RSATest.Q07F_FEDUP_RSA);
        VirtuosoTest.executeOnVirtuoso(RSATest.Q07F_RSA);
    }

    public static String Q07F_FEDUP_RSA = """
            SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2
            WHERE
              {   {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
                                          { ?localProduct
                                                      a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                                            ?localProduct
                                                      <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                                            ?localProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                          }
                                        OPTIONAL
                                          {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
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
                                                            UNION
                                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
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
                                                          }
                                                        UNION
                                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
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
                                                      }
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
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
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
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
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
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
                                          }
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
                                          { ?localProduct
                                                      a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                                            ?localProduct
                                                      <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                                            ?localProduct
                                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                          }
                                        OPTIONAL
                                          {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
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
                                                            UNION
                                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
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
                                                          }
                                                        UNION
                                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
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
                                                      }
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
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
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
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
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
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
                                          }
                                      }
                                  }
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.vendor47.fr>
                                      { ?localProduct
                                                  a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                                        ?localProduct
                                                  <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                                        ?localProduct
                                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                      }
                                    OPTIONAL
                                      {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
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
                                                        UNION
                                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
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
                                                      }
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
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
                                                  }
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
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
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
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
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
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
                                      }
                                  }
                              }
                            UNION
                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
                                  { ?localProduct
                                              a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                                    ?localProduct
                                              <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                                    ?localProduct
                                              <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                                  }
                                OPTIONAL
                                  {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
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
                                                    UNION
                                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
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
                                                  }
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
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
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
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
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
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
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
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
                                  }
                              }
                          }
                        UNION
                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
                              { ?localProduct
                                          a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                                ?localProduct
                                          <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                                ?localProduct
                                          <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                              }
                            OPTIONAL
                              {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
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
                                                UNION
                                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
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
                                              }
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
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
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
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
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
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
                                  }
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
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
                              }
                          }
                      }
                    UNION
                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
                          { ?localProduct
                                      a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                            ?localProduct
                                      <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                            ?localProduct
                                      <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                          }
                        OPTIONAL
                          {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
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
                                            UNION
                                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
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
                                          }
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
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
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
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
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
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
                              }
                            UNION
                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
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
                          }
                      }
                  }
                UNION
                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
                      { ?localProduct
                                  a  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product> .\s
                        ?localProduct
                                  <http://www.w3.org/2000/01/rdf-schema#label>  ?productLabel .\s
                        ?localProduct
                                  <http://www.w3.org/2002/07/owl#sameAs>  <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Product72080>
                      }
                    OPTIONAL
                      {   {   {   {   {   { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite78.fr>
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
                                        UNION
                                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite34.fr>
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
                                      }
                                    UNION
                                      { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite98.fr>
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
                                  }
                                UNION
                                  { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr>
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
                            UNION
                              { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite87.fr>
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
                          }
                        UNION
                          { SERVICE SILENT <http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite76.fr>
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
                      }
                  }
              }
            """;

}
