package fr.gdd.fedup;

import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.cli.*;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.mgt.Explain;

/**
 * FedUP server that runs on top of Apache Jena Fuseki.
 */
public class FedUPServer {

    public static void main(String[] args) throws ParseException {
        Options options = new Options();

        options.addOption(new Option("h", "help", false,
                "print this message"));
        options.addOption("s", "summary", true,
                "The path to the summary as TDB2 dataset.");
        // options.addOption("t", "type", true, "The summary type (example: ModuloOnSuffix(1)).");
        options.addOption("e", "engine", true,
                "The federation engine in charge of executing (default: Jena; FedX).");
        options.addOption("x", "export", false,
                "The federated query plan is exported within HTTP responses. (default: false)");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("fedup [--engine <engine>] --sumary <path> --type <type>", options);
            return;
        }

        // TODO parse the arg
        // TODO or encode it within the dataset by default.
        ModuloOnSuffix strategy = new ModuloOnSuffix(1);
        Summary s = new Summary(strategy, Location.create(cmd.getOptionValue("s")));

        // TODO explain level as argument
        ARQ.setExecutionLogging(Explain.InfoLevel.ALL);

        // Export the results in the HTTP response?
        s.getSummary().getContext().set(FedUPConstants.EXPORT_PLANS, cmd.hasOption("x"));
        // Which engine use once the sources are assigned?
        if (cmd.hasOption("e")) {
            s.getSummary().getContext().set(FedUPConstants.EXECUTION_ENGINE, cmd.getOptionValue("e"));
        } else {
            s.getSummary().getContext().set(FedUPConstants.EXECUTION_ENGINE, FedUPConstants.APACHE_JENA);
        }

        FedUPEngine.register();

        FusekiServer.create()
                .add("summary", s.getSummary()) // make it accessible, when queried, it makes federated query
                .build()
                .start();
    }

}
