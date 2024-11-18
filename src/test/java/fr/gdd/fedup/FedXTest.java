package fr.gdd.fedup;

import fr.gdd.fedup.summary.InMemorySummaryFactory;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.jena.datatypes.BaseDatatype;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.impl.XSDDouble;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.tdb2.sys.TDBInternal;
import org.eclipse.rdf4j.federated.FedXConfig;
import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.federated.repository.FedXRepository;
import org.eclipse.rdf4j.federated.repository.FedXRepositoryConnection;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.rio.datatypes.RDFDatatypeHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;

/**
 * Testing another federated query engine, namely FedX. FedX is meant
 * for federated query execution and therefore benefits from some
 * optimizations.
 */
public class FedXTest {

    Logger log = LoggerFactory.getLogger(FedXTest.class);

    static Dataset dataset;
    static Summary summary;
    static List<String> graphs = List.of("https://graphA.org", "https://graphB.org");
    static List<String> endpoints = List.of("http://localhost:3333/graphA/sparql", "http://localhost:3334/graphB/sparql");


    @BeforeAll
    public static void initialize_dataset() {
        InMemorySummaryFactory imsf = new InMemorySummaryFactory();
        dataset = imsf.getPetsDataset();
        summary = imsf.getSimplePetsSummary();
    }

    @AfterAll
    public static void drop_dataset() {
        TDBInternal.expel(dataset.asDatasetGraph());
        TDBInternal.expel(summary.getSummary().asDatasetGraph());
    }

    @Test
    public void fedup_to_fedx_with_optional() {
        executeFedUPThenFedX("""
                SELECT * WHERE {
                    <http://auth/person> <http://auth/named> ?person .
                    OPTIONAL { ?person <http://auth/owns> ?animal }
                }""");
    }

    @Test
    public void fedup_to_fedx_with_filter_to_see_if_they_are_pushed_down() {
        executeFedUPThenFedX("""
            SELECT DISTINCT * WHERE {
            { ?people <http://auth/owns> ?animal }
              FILTER ( ?people = <http://auth/dog> )
            }""");
    }

    @Test
    public void fedx_with_filter() {
        // filters are effectively pushed down. BUT limit and distinct aren't…
        executeWithFedX("""
                SELECT DISTINCT  *
                WHERE
                  {   { SERVICE SILENT <http://localhost:3333/graphA/sparql>
                          { ?people  <http://auth/owns>  ?animal}
                      }
                    UNION
                      { SERVICE SILENT <http://localhost:3334/graphB/sparql>
                          { ?people  <http://auth/owns>  ?animal}
                      }
                    FILTER ( ?animal = <http://auth/dog> )
                  }
                  """);
        // QueryRoot
        //   Distinct
        //      Projection
        //         ProjectionElemList
        //            ProjectionElem "people"
        //            ProjectionElem "animal"
        //         NUnion
        //            ExclusiveStatement
        //               Var (name=people)
        //               Var (name=_const_4ea587e2_uri, value=http://auth/owns, anonymous)
        //               Var (name=animal, value=http://auth/dog)
        //               StatementSource (id=sparql_localhost:3333_graphA_sparql, type=REMOTE)
        //               BoundFilters (animal=http://auth/dog)
        //            ExclusiveStatement
        //               Var (name=people)
        //               Var (name=_const_4ea587e2_uri, value=http://auth/owns, anonymous)
        //               Var (name=animal, value=http://auth/dog)
        //               StatementSource (id=sparql_localhost:3334_graphB_sparql, type=REMOTE)
        //               BoundFilters (animal=http://auth/dog)
    }

    /* ***************************************************************** */

    public void executeFedUPThenFedX(String queryAsString) {
        FedUP fedup = new FedUP(summary, dataset).shouldNotFactorize();
        String result = fedup.query(queryAsString, new HashSet<>(graphs));
        // so we need to replace
        result = result.replace(graphs.get(0), endpoints.get(0))
                .replace(graphs.get(1), endpoints.get(1));

        List<FusekiServer> servers = FedUPTest.startServers();
        executeWithFedX(result);
        FedUPTest.stopServers(servers);
    }


    public MultiSet<org.apache.jena.sparql.engine.binding.Binding> executeWithFedX(String queryAsString) {
        // still need dataset since the dataset refers to <graphA> and not
        // endpoint remote address.
        MultiSet<org.apache.jena.sparql.engine.binding.Binding> bindings = new HashMultiSet<>();

        FedXConfig config = new FedXConfig();
        if (log.isDebugEnabled()) {
            config.withDebugQueryPlan(true); // in std out…
        }

        FedXRepository fedx = FedXFactory.newFederation()
                .withConfig(config)
                .create();

        try (FedXRepositoryConnection conn = fedx.getConnection()) {
            TupleQuery tq = conn.prepareTupleQuery(queryAsString);

            try (TupleQueryResult tqRes = tq.evaluate()) {
                while (tqRes.hasNext()) {
                    bindings.add(createBinding(tqRes.next()));
                }

                log.debug("Got {} results.", bindings.size());
            }

        }

        return bindings;
    }

    /**
     * @param origin The RDF4J binding to convert.
     * @return An Apache Jena `Binding` that comes from an RDF4J binding.
     */
    public static org.apache.jena.sparql.engine.binding.Binding createBinding(BindingSet origin) {
        BindingBuilder builder = BindingFactory.builder();
        for (String name : origin.getBindingNames()) {
            Binding value = origin.getBinding(name);
            Node valueAsNode = null;
            if (value.getValue().isBNode()) {
                valueAsNode = NodeFactory.createBlankNode(value.getValue().stringValue());
            } else if (value.getValue().isIRI()) {
                valueAsNode = NodeFactory.createURI(value.getValue().stringValue());
            } else if (value.getValue().isLiteral()) {
                if (value.getValue().toString().contains(XSDDatatype.XSDinteger.getURI())) {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue(), XSDDatatype.XSDinteger);
                } else if (value.getValue().toString().contains(XSDDatatype.XSDdouble.getURI())) {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue(), XSDDatatype.XSDdouble);
                } else if (value.getValue().toString().contains(XSDDatatype.XSDdateTime.getURI())) {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue(), XSDDatatype.XSDdateTime);
                } else {
                    valueAsNode = NodeFactory.createLiteral(value.getValue().stringValue());
                }
            } else if (value.getValue().isResource() || value.getValue().isTriple()) {
                throw new UnsupportedOperationException("RDF4J to Jena Bindings with a resource or a triple.");
            }
            builder.add(Var.alloc(name), valueAsNode);
        }
        return builder.build();
    }
}
