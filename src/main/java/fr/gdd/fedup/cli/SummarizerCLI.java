package fr.gdd.fedup.cli;

import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.summary.SummaryFactory;
import org.apache.commons.cli.ParseException;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.*;

/**
 * A class that contains a main to enable easy ingestion of
 * data to produce FedUP summary depending on arguments.
 */
@picocli.CommandLine.Command(
        name = "summarizer",
        version = "0.0.2",
        description = "Creates the TDB2 summary for FedUP.",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class SummarizerCLI {

    @picocli.CommandLine.Option(
            order = 2,
            names = {"-i", "--input"},
            paramLabel = "path/to/tdb2 | http://remote/endpoint",
            description = "The path to the TDB2 dataset to summarize.")
    public String input;

    @picocli.CommandLine.Option(
            order = 2,
            names = {"-o", "--output"},
            paramLabel = "path/to/tdb2",
            description = "The path to the TDB2 dataset summarized.")
    public String output;

    @picocli.CommandLine.Option(
            order = 3,
            names = {"--hash"},
            paramLabel = "0",
            description = "The modulo value of the hash that summarizes (default: 0).")
    public int hash = 0;


    public static void main(String[] args) throws ParseException {
        SummarizerCLI options = new SummarizerCLI();
        try {
            new picocli.CommandLine(options).parseArgs(args);
        } catch (Exception e) {
            picocli.CommandLine.usage(options, System.out);
            System.exit(picocli.CommandLine.ExitCode.USAGE);
        }

        Path outputAsPath = Path.of(options.output);
        if (!outputAsPath.toFile().isDirectory()) {
            boolean createFolder = outputAsPath.toFile().mkdirs();
            if (!createFolder) {
                System.out.printf("There was an issue while creating the output TDB2 folder %s.%n", outputAsPath.getFileName().toString());
                return;
            }
        };

        // TODO output is a service as well, that we update using UPDATE

        Summary summary = SummaryFactory.createModuloOnSuffix(options.hash, Location.create(outputAsPath));

        // #A if remote input, create a service query
        try {
            // Instead of getting the ?g ?s ?p ?o, we retrieve the graphs
            // first to subdivide the query into smaller ?s ?p ?o queries.
            Set<String> graphs = getGraphs(options.input);
            List<String> graphsAdded = new ArrayList<>();
            graphs.forEach(graph -> {
                graphsAdded.add(graph);
                System.out.printf("%s: Started summarizing %s…%n", graphsAdded.size(), graph);
                List<Quad> quads2add = new ArrayList<>();
                List<Triple> triples = getSPO(options.input, graph);
                triples.forEach(triple -> {
                    quads2add.add(Quad.create(NodeFactory.createURI(graph), triple));
                });
                System.out.printf("%s: Summarizing %s triples…%n", graphsAdded.size(), quads2add.size());

                summary.add(quads2add.iterator());

                summary.getSummary().begin(ReadWrite.READ);
                int graphSize = summary.getSummary().asDatasetGraph().getGraph(NodeFactory.createURI(graph)).size();
                summary.getSummary().commit();
                summary.getSummary().close();

                System.out.printf("%s: Summary contains %s triples for this graph.%n", graphsAdded.size(), graphSize);
            });

            System.out.printf("Number of graphs added in the summary: %s.%n", graphsAdded.size());

        } catch (NullPointerException | IOException e) {
            System.err.println("Could not open or access the input dataset.");
            System.exit(CommandLine.ExitCode.USAGE);
        }

        summary.getSummary().begin(ReadWrite.READ);
        System.out.printf("Number of statements: %s.%n", summary.getSummary().getUnionModel().size());
        summary.getSummary().commit();
        summary.getSummary().close();
    }

    /* ***************************************************************************** */

    public static Set<String> getGraphs (String pathOrUri) throws IOException {
        try {
            // We try as a URI
            URI inputURI = new URI(pathOrUri);
            return getGraphsFromService(inputURI);
        } catch (URISyntaxException e) {
            // otherwise it should be a TDB2
            Path inputAsPath = Path.of(pathOrUri);
            if (!inputAsPath.toFile().isDirectory()) {
                throw new IOException("The directory is not a TDB2 folder.");
            }
            Dataset inputDataset = TDB2Factory.connectDataset(Location.create(inputAsPath));
            return getGraphsFromTDB2(inputDataset);
        }
    }

    public static List<Triple> getSPO (String pathOrUri, String graph) {
        try {
            // We try as a URI
            URI inputURI = new URI(pathOrUri);
            return getSPOFromServiceGraph(inputURI, graph);
        } catch (URISyntaxException e) {
            // otherwise it should be a TDB2
            Path inputAsPath = Path.of(pathOrUri); // would throw before
            Dataset inputDataset = TDB2Factory.connectDataset(Location.create(inputAsPath));
            return getSPOFromTDB2Graph(inputDataset, graph);
        }
    }

    /* ***************************************************************************** */

    public static Set<String> getGraphsFromTDB2 (Dataset dataset) {
        dataset.begin(ReadWrite.READ);
        Iterator<Node> graphs = dataset.asDatasetGraph().listGraphNodes();
        dataset.end();
        Set<String> results = new HashSet<>();
        while (graphs.hasNext()) {
            Node graphNode = graphs.next();
            results.add(graphNode.getURI());
        }
        return results;
    }

    public static List<Triple> getSPOFromTDB2Graph (Dataset dataset, String graph) {
        dataset.begin(ReadWrite.READ);
        List<Triple> triples = dataset.asDatasetGraph().getGraph(NodeFactory.createURI(graph))
                .stream()
                .map(t ->
                        Triple.create(t.getSubject(), t.getPredicate(), t.getObject())
                ).toList();
        dataset.end();
        return triples;
    }


    /**
     * @param uri The service to summarize.
     * @return The set of graphs served by the service.
     */
    public static Set<String> getGraphsFromService(URI uri) {
        Query getGraphsQuery = QueryFactory.create(String.format("""
                           SELECT DISTINCT ?g WHERE { SERVICE <%s> {
                             SELECT DISTINCT ?g WHERE {GRAPH ?g {?s ?p ?o}}
                           } }
                           """, uri));

        try (QueryExecution qexec = QueryExecutionFactory.create(getGraphsQuery, DatasetFactory.empty())) {
            ResultSet results = qexec.execSelect();
            Set<String> onlyGraphs = new HashSet<>();
            while (results.hasNext()) {
                QuerySolution result = results.nextSolution();
                onlyGraphs.add(result.get("?g").toString());
            }
            return onlyGraphs;
        } catch (Exception e) {
            System.err.println("Could not get graphs from " + uri);
        }
        return null;
    }


    /**
     * @param uri The service uri.
     * @param graph The graph from the service uri.
     * @return The triples from the targeted graph at targeted uri.
     */
    public static List<Triple> getSPOFromServiceGraph (URI uri, String graph) {
        Query getGraphsQuery = QueryFactory.create(String.format("""
                           SELECT DISTINCT ?s ?p ?o WHERE { SERVICE <%s> {
                             GRAPH <%s> { ?s ?p ?o }
                           } }
                           """, uri, graph));

        try (QueryExecution qexec = QueryExecutionFactory.create(getGraphsQuery, DatasetFactory.empty())) {
            ResultSet results = qexec.execSelect();
            List<Triple> spos = new ArrayList<>();
            while (results.hasNext()) {
                QuerySolution result = results.nextSolution();
                spos.add(Triple.create(result.get("?s").asNode(), result.get("?p").asNode(), result.get("?o").asNode()));
            }
            return spos;
        } catch (Exception e) {
            System.err.println("Could not get SPO for graph " + graph + " from " + uri);
        }
        return null;
    }

}
