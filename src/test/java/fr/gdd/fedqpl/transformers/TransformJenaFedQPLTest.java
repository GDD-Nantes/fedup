package fr.gdd.fedqpl.transformers;

import fr.gdd.fedqpl.operators.FedQPLOpSet;
import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.summary.SummaryFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransformJenaFedQPLTest {

    Logger log = LoggerFactory.getLogger(TransformJenaFedQPLTest.class);

    @Test
    public void simple_fedqpl_from_sparql_triple_and_summary () {
        Summary ims = InMemorySummaryFactory.getSimplePetsSummary();

        String queryString = """
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?p .
                }""";

        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);

        op = ims.transform(op);
        // as if the whole summary was built using random walks
        TransformJenaFedQPL toFedQPL = new TransformJenaFedQPL(ims.getSummary());
        FedQPLOpSet fedQPL = (FedQPLOpSet) Transformer.transform(toFedQPL, op);

        assertEquals(2, fedQPL.size()); // 2 graphs found: graphA and graphB
    }

    @Test
    public void simple_fedqpl_from_sparql_bgp_and_summary () {
        Summary ims = InMemorySummaryFactory.getSimplePetsSummary();
        String queryString = """
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?p .
                    ?p <http://auth/owns> ?a
                }""";

        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);

        op = ims.transform(op);
        // as if the whole summary was built using random walks
        TransformJenaFedQPL toFedQPL = new TransformJenaFedQPL(ims.getSummary());
        FedQPLOpSet fedQPL = (FedQPLOpSet) Transformer.transform(toFedQPL, op);

        // Everything joins with everything since all URI are mapped to <http://auth/0>
        // so we get A x A -- A x B -- B x A -- B x B
        assertEquals(4, fedQPL.size());
    }

    @Test
    public void simple_fedqpl_from_sparql_bgp_and_dataset_id_not_summary () {
        Dataset id = InMemorySummaryFactory.getPetsDataset();
        String queryString = """
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?p .
                    ?p <http://auth/owns> ?a
                }""";

        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);

        TransformJenaFedQPL toFedQPL = new TransformJenaFedQPL(id);
        FedQPLOpSet fedQPL = (FedQPLOpSet) Transformer.transform(toFedQPL, op);

        // Alice@A -> cat@A -- David@B -> dog@B
        assertEquals(2, fedQPL.size());
    }


    @Disabled
    @Test
    public void testNaivePerformSourceSelection () {
        List<String> endpoints = List.of(
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite8.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor0.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite7.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite6.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor2.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor6.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor4.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite0.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor8.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor5.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor9.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite4.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite9.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite5.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor1.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite2.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite1.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor3.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.ratingsite3.fr/",
            "http://localhost:5555/sparql?default-graph-uri=http://www.vendor7.fr/"
        );

        String queryString = """
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            
            SELECT DISTINCT ?product ?label WHERE { 
                ?product rdfs:label ?label .
                
                ?product rdf:type ?localProductType .
                ?localProductType owl:sameAs bsbm:ProductType647 .
            
                ?product bsbm:productFeature ?localProductFeature1 . 
                ?localProductFeature1 owl:sameAs bsbm:ProductFeature8774 .
            
                ?product bsbm:productFeature ?localProductFeature2 . 
                ?localProductFeature2 owl:sameAs bsbm:ProductFeature16935 .
                ?product bsbm:productPropertyNumeric1 ?value1 . 
                
                FILTER (?value1 > "744"^^xsd:integer) 
            }
            ORDER BY ?product ?label
            LIMIT 10
        """;


        // Generate algebra of the query
        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);

        /*TransformJenaFedQPL transform = new TransformJenaFedQPL(endpoints);
        Mu fedQPLPlan = (Mu) Transformer.transform(transform, op);

        PrinterVisitor pv = new PrinterVisitor();
        pv.visit(fedQPLPlan);
        pv.pprint();*/
        
        /* 
            TO DO, assertEquals fedQPLPlan to a manually built fedQPLPlan

            - Parse using ARQ: ./apache-jena-4.7.0/bin/qparse --print op --query queries/fedshop/q01.sparql
            - Parse results
                (prefix ((xsd: <http://www.w3.org/2001/XMLSchema#>)
                (bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>)
                (rdfs: <http://www.w3.org/2000/01/rdf-schema#>)
                (rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>)
                (owl: <http://www.w3.org/2002/07/owl#>)
                (bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>))

                (slice _ 10
                    (distinct
                    (project (?product ?label)
                        (order (?product ?label)
                        (filter (> ?value1 744)
                            (bgp
                            (triple ?product rdfs:label ?label)
                            (triple ?product rdf:type ?localProductType)
                            (triple ?localProductType owl:sameAs bsbm:ProductType647)
                            (triple ?product bsbm:productFeature ?localProductFeature1)
                            (triple ?localProductFeature1 owl:sameAs bsbm:ProductFeature8774)
                            (triple ?product bsbm:productFeature ?localProductFeature2)
                            (triple ?localProductFeature2 owl:sameAs bsbm:ProductFeature16935)
                            (triple ?product bsbm:productPropertyNumeric1 ?value1)
                            )))))))
            
        */
        // Mu expected = new Mu();

        // BasicPattern bgp = new BasicPattern();
        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("product"), 
        //     NodeFactory.createURI("http://www.w3.org/2000/01/rdf-schema#label"), 
        //     NodeFactory.createVariable("label")
        // ));
        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("product"), 
        //     NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
        //     NodeFactory.createVariable("localProductType")
        // ));
        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("localProductType"), 
        //     NodeFactory.createURI("http://www.w3.org/2002/07/owl#sameAs"), 
        //     NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductType647")
        // ));

        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("product"), 
        //     NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature"), 
        //     NodeFactory.createVariable("localProductFeature1")
        // ));

        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("localProductFeature1"), 
        //     NodeFactory.createURI("http://www.w3.org/2002/07/owl#sameAs"), 
        //     NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature8774")
        // ));

        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("product"), 
        //     NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature"), 
        //     NodeFactory.createVariable("localProductFeature2")
        // ));

        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("localProductFeature2"), 
        //     NodeFactory.createURI("http://www.w3.org/2002/07/owl#sameAs"), 
        //     NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/ProductFeature16935")
        // ));

        // bgp.add(Triple.create(
        //     NodeFactory.createVariable("product"), 
        //     NodeFactory.createURI("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1"), 
        //     NodeFactory.createVariable("value1")
        // ));

        // Filter filter = new Filter(
        //     new ExprList(new E_GreaterThan(new ExprVar("?value1"), new NodeValueInteger(744))),
        //     new OpBGP(bgp)
        // );
    }

}