package fr.gdd.fedup.cli;

import fr.gdd.fedup.summary.Summary;
import fr.gdd.fedup.summary.SummaryFactory;
import org.apache.commons.cli.ParseException;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.http.auth.AuthEnv;
import org.apache.jena.query.*;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.modify.request.UpdateModify;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.update.UpdateRequest;
import picocli.CommandLine;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            paramLabel = "<path/to/tdb2 | http://input/endpoint>",
            required = true,
            description = "Set the dataset to summarize.")
    public String input;

    @picocli.CommandLine.Option(
            order = 2,
            names = {"-o", "--output"},
            paramLabel = "<path/to/tdb2 | http://output/endpoint>",
            required = true,
            description = "Set the output summary dataset.")
    public String output;

    @picocli.CommandLine.Option(
            order = 3,
            names = {"-u", "--username"},
            paramLabel = "<$USERNAME>",
            description = "(Not tested) The username for the summary database if needed.")
    public String username;

    @picocli.CommandLine.Option(
            order = 3,
            names = {"-p", "--password"},
            paramLabel = "<$PASSWORD>",
            description = "(Not tested) The password for the summary database if needed.")
    public String password;

    @picocli.CommandLine.Option(
            order = 4,
            names = {"--hash"},
            paramLabel = "0",
            description = "The modulo value of the hash that summarizes (default: 0).")
    public int hash = 0;

    @picocli.CommandLine.Option(
            order = 5,
            names = {"--filter"},
            paramLabel = ".*",
            description = "The regular expression to filter out read graphs.")
    public String filterRegex = ".*"; // by default allows everything

    public static void main(String[] args) throws ParseException {
        // http://www.w3.org/2002/07/owl#…

        SummarizerCLI options = new SummarizerCLI();
        try {
            new picocli.CommandLine(options).parseArgs(args);
        } catch (Exception e) {
            picocli.CommandLine.usage(options, System.out);
            System.exit(picocli.CommandLine.ExitCode.USAGE);
        }

        Pattern pattern = Pattern.compile(options.filterRegex);


        if (Objects.nonNull(options.password) && Objects.nonNull(options.username)) {
            try {
                URI remoteSummary = new URI(options.output);
                AuthEnv.get().registerUsernamePassword(remoteSummary, options.username, options.password);
            } catch (URISyntaxException e) {
                System.err.println("Error with login/password info for the remote summary.");
                System.exit(CommandLine.ExitCode.USAGE);
            }
        }

        Path outputAsPath = Paths.get(options.output);
        Summary summary = outputAsPath.toFile().isDirectory() ?
                SummaryFactory.createModuloOnSuffix(options.hash, Location.create(outputAsPath)):
                SummaryFactory.createModuloOnSuffix(options.hash);

        // #A if remote input, create a service query
        try {
            // Instead of getting the ?g ?s ?p ?o, we retrieve the graphs
            // first to subdivide the query into smaller ?s ?p ?o queries.
            Set<String> graphs = getGraphs(options.input);
            System.out.println("Number of graphs to summarize: " + graphs.size());
            List<String> graphsAdded = new ArrayList<>();
            graphs.forEach(graph -> {
                Matcher matcher = pattern.matcher(graph);

                if (matcher.matches()) {
                    graphsAdded.add(graph);
                    System.out.printf("%s: Started summarizing %s…%n", graphsAdded.size(), graph);
                    List<Quad> quads2add = new ArrayList<>();
                    List<Triple> triples = getSPO(options.input, graph);
                    triples.forEach(triple -> {
                        quads2add.add(Quad.create(NodeFactory.createURI(graph), triple));
                    });
                    System.out.printf("%s: Summarizing %s triples…%n", graphsAdded.size(), quads2add.size());

                    int nbSummarized = updateSummary(options.output, graph, triples, summary);

                    System.out.printf("%s: Summary contains %s triples for this graph.%n", graphsAdded.size(), nbSummarized);
                }
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

    public static int updateSummary (String pathOrUri, String graph, List<Triple> triples, Summary summary) {
        List<Quad> quads2summarize = triples.stream().map(t -> Quad.create(NodeFactory.createURI(graph), t)).toList();

        try {
            URI outputURI = new URI(pathOrUri);

            Set<Quad> summarized = summary.toAdd(quads2summarize.iterator());
            UpdateModify updateModify = new UpdateModify();
            summarized.forEach(q-> updateModify.getInsertAcc().addQuad(q));
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.add(updateModify);

            HttpClient httpClient = HttpClient.newBuilder().build();

            try ( RDFConnection conn = RDFConnectionRemote.service(pathOrUri)
                    .httpClient(httpClient)
                    .build()) {
                conn.update(updateRequest);
            }
            return summarized.size();
        } catch (URISyntaxException e) {
            // otherwise, we try to create a local TDB2 database
            return summary.add(quads2summarize.iterator());
        }
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
