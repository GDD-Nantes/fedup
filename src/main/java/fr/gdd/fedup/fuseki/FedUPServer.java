package fr.gdd.fedup.fuseki;

import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.mgt.Explain;
import org.checkerframework.checker.units.qual.A;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * FedUP server that runs on top of Apache Jena Fuseki. All options available are
 * printed with the `--help` command.
 */
public class FedUPServer {

    public static void main(String[] args) throws ParseException {
        Options options = new Options();

        options.addOption(new Option("h", "help", false,
                "print this message"));

        Option summariesOpt = new Option("s", "summaries", true,
                "Path(s) to TDB2 dataset summary(ies).");
        summariesOpt.setArgs(Option.UNLIMITED_VALUES);
        summariesOpt.setValueSeparator(',');
        options.addOption(summariesOpt);

        // options.addOption("t", "type", true, "The summary type (example: ModuloOnSuffix(1)).");
        options.addOption("e", "engine", true,
                "The federation engine in charge of executing (default: Jena; FedX).");
        options.addOption("x", "export", false,
                "The federated query plan is exported within HTTP responses (default: false).");
        options.addOption("p", "port", true,
                "The port of this FedUP server (default: 3330).");
        // TODO Add option to configure remote virtuoso


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        
        if (cmd.hasOption("help") || cmd.getOptions().length==0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("fedup [options] --sumaries <path>", options);
            return;
        }

        // TODO create a fedup factory
        // TODO parse the arg
        // TODO or encode it within the dataset by default.
        ModuloOnSuffix strategy = new ModuloOnSuffix(1);

        List<Pair<String, Summary>> summaries = new ArrayList<>();
        for (Path path: Arrays.stream(cmd.getOptionValues('s')).map(Path::of).toList()) {
            Summary s = new Summary(strategy, Location.create(path));
            // Export the results in the HTTP response?
            s.getSummary().getContext().set(FedUPConstants.EXPORT_PLANS, cmd.hasOption("x"));
            // Which engine use once the sources are assigned?
            if (cmd.hasOption("e")) {
                s.getSummary().getContext().set(FedUPConstants.EXECUTION_ENGINE, cmd.getOptionValue("e"));
            } else {
                s.getSummary().getContext().set(FedUPConstants.EXECUTION_ENGINE, FedUPConstants.APACHE_JENA);
            }
            s.getSummary().getContext().set(ARQ.optimization, false);
            summaries.add(new ImmutablePair<>(path.getFileName().toString(), s));
        }

        ARQ.setExecutionLogging(Explain.InfoLevel.ALL);  // TODO explain level as argument
        int port = cmd.hasOption("p") ? Integer.parseInt(cmd.getOptionValue("p")) : 3330; // On which port?
        FedUPEngine.register();
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, FedUPPlanAndNormalJSON.factory);

        var builder = FusekiServer.create()
                .port(port)
                .enableCors(true)
                .verbose(true);

        for (Pair<String, Summary> nameAndSummary : summaries) {
            System.out.println("Summary available: " + nameAndSummary.getLeft());
            builder.add(nameAndSummary.getLeft(), nameAndSummary.getRight().getSummary());
        }

        builder.build().start();
    }

}
