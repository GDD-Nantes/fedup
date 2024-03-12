package fr.gdd.fedup.summary;

import org.apache.commons.cli.*;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;

import java.nio.file.Path;
import java.util.Iterator;

/**
 * A class that contains a main to enable easy ingestion of
 * data to produce FedUP summary depending on arguments.
 *
 * TODO input could be the address to a remote endpoint, unfortunately,
 * TODO Apache Jena + service + quad do not work well together…
 */
public class SummaryIngester {

    // mvn exec:java -Dmain.class="fr.gdd.fedup.summary.SummaryIngester"
    public static void main(String[] args) throws ParseException {
        Options options = new Options();

        options.addOption(new Option("h", "help", false, "print this message"));

        options.addOption(new Option("hash",true,"The modulo value of the hash that summarizes (default: 0)."));
        options.addOption(new Option("i", "input", true, "The path to the TDB2 dataset to summarize."));
        options.addOption(new Option("o", "output", true, "The path to the TDB2 dataset summarized."));

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help") || cmd.getOptions().length==0 || !cmd.hasOption("input") || !cmd.hasOption("output")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("fedup-ingester -i <path> -o <path>", options);
            return;
        }

        Path inputAsPath = Path.of(cmd.getOptionValue("input"));
        if (!inputAsPath.toFile().isDirectory()) {
            System.out.printf("Input TDB2 folder %s does not seem to exist…%n", inputAsPath.getFileName().toString());
            return;
        }

        Path outputAsPath = Path.of(cmd.getOptionValue("output"));
        if (!inputAsPath.toFile().isDirectory()) {
            boolean createFolder = outputAsPath.toFile().mkdirs();
            if (!createFolder) {
                System.out.printf("There was an issue while creating the output TDB2 folder %s.%n", outputAsPath.getFileName().toString());
                return;
            }
        };

        int hashModulo = Integer.parseInt(cmd.getOptionValue("hash", "0" ));
        Summary summary = SummaryFactory.createModuloOnSuffix(hashModulo, Location.create(outputAsPath));

        Dataset inputDataset = TDB2Factory.connectDataset(Location.create(inputAsPath));

        inputDataset.begin(ReadWrite.READ);
        Iterator<Node> graphs = inputDataset.asDatasetGraph().listGraphNodes();
        int nbGraphs = 0;
        while (graphs.hasNext()) {
            Node graphNode = graphs.next();
            ++nbGraphs;
            System.out.printf("%s: Started summarizing %s…%n", nbGraphs, graphNode.getURI());
            Iterator<Quad> quads = inputDataset.asDatasetGraph().getGraph(graphNode).stream().map(t ->
                    Quad.create(graphNode, t.getSubject(), t.getPredicate(), t.getObject())
            ).iterator();

            summary.getSummary().begin(ReadWrite.READ);
            int graphSize = summary.getSummary().asDatasetGraph().getGraph(graphNode).size();
            summary.getSummary().commit();
            summary.getSummary().close();

            System.out.printf("%s: Summary contains %s triples for this graph.%n", nbGraphs, graphSize);

            summary.add(quads);
        }
        inputDataset.commit();
        inputDataset.close();
        System.out.printf("Number of graphs in summary: %s.%n", nbGraphs);

        summary.getSummary().begin(ReadWrite.READ);
        System.out.printf("Number of statements: %s.%n", summary.getSummary().getUnionModel().size());
        summary.getSummary().commit();
        summary.getSummary().close();
    }
}
