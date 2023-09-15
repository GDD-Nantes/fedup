package fr.gdd.fedqpl.transformers;

import fr.gdd.fedqpl.operators.FedQPLOperator;
import fr.gdd.fedqpl.operators.Filter;
import fr.gdd.fedqpl.operators.Mu;
import fr.gdd.fedqpl.visitors.PrinterVisitor;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.expr.E_GreaterThan;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.junit.jupiter.api.Test;

import java.util.List;

class OpTransformJenaTest {

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

        OpTransformJena transform = new OpTransformJena(endpoints);
        Mu fedQPLPlan = (Mu) Transformer.transform(transform, op);

        PrinterVisitor pv = new PrinterVisitor();
        pv.visit(fedQPLPlan);
        pv.pprint();
        
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