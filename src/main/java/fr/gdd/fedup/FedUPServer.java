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

        options.addOption(new Option("h", "help", false, "print this message"));
        options.addOption("s", "summary", true, "The path to the summary as TDB2 dataset.");
        options.addOption("t", "type", true, "The summary type (example: ModuloOnSuffix(1)).");
        options.addOption("e", "engine", false, "The federation engine in charge of executing (default: Apache Jena).");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("fedup [--engine <engine>] --sumary <path> --type <type>", options);
            return;
        }

        ModuloOnSuffix strategy = new ModuloOnSuffix(1); // TODO parse the arg
        Summary s = new Summary(strategy, Location.create(cmd.getOptionValue("s")));

        ARQ.setExecutionLogging(Explain.InfoLevel.ALL);

        FedUPEngine.register();

        FusekiServer.create()
                .add("summary", s.getSummary()) // make it accessible, when queried, it makes federated query
                .build().start();
    }

}
