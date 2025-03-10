package fr.gdd.fedup.cli;

import fr.gdd.fedup.fuseki.FedUPConstants;
import fr.gdd.fedup.fuseki.FedUPEngine;
import fr.gdd.fedup.fuseki.FedUPPlanAndNormalJSON;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.resultset.ResultSetReaderRegistry;
import org.apache.jena.riot.rowset.RowSetWriterRegistry;
import org.apache.jena.sparql.engine.main.QC;
import picocli.CommandLine;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * FedUP server that runs on top of Apache Jena Fuseki. All options available are
 * printed with the `--help` command.
 */
@picocli.CommandLine.Command(
        name = "fedup-server",
        version = "0.0.2",
        description = "Federation engine as a server for SPARQL query processing.",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class FedUPServerCLI {

    @picocli.CommandLine.Option(
            order = 2,
            names = {"-p", "--port"},
            paramLabel = "3330",
            description = "The port of this FedUP server.")
    int port = 3330;

    @picocli.CommandLine.Option(
            order = 3,
            names = {"-s", "--summaries"},
            split = ",",
            required = true,
            paramLabel = "<path/to/tdb2 | http://output/endpoint>",
            description = "Path to the summary dataset(s).")
    List<String> summaryPaths;

    @picocli.CommandLine.Option(
            order = 4,
            required = true,
            names = {"-e", "--engine"},
            paramLabel = "None | Jena | FedX",
            description = "The federation engine in charge of executing (default: None).")
    String engine;

    @picocli.CommandLine.Option(
            order = 4,
            names = {"-x", "--export"},
            paramLabel = "false",
            description = "The federated query plan is exported within HTTP responses.")
    Boolean export = false;

    @picocli.CommandLine.Option(
            order = 6,
            names = {"-m", "--modify"},
            paramLabel = "(e) -> \"http://localhost:5555/sparql?default-graph-uri=\"+(e.substring(0, e.length() - 1))",
            description = "Lambda expression to apply to graphs in summaries in order to call actual endpoints.")
    String modifyEndpoints = "(e) -> \"http://localhost:5555/sparql?default-graph-uri=\"+(e.substring(0, e.length() - 1))";

    @picocli.CommandLine.Option(
            order = 7,
            names = {"--filter"},
            paramLabel = ".*",
            description = "The regular expression to filter out read endpoints.")
    public String filterRegex = ".*"; // by default allows everything

    @picocli.CommandLine.Option(
            order = Integer.MAX_VALUE, // last
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Display this help message.")
    boolean usageHelpRequested;

    public static void main(String[] args) throws ParseException {
        FedUPServerCLI options = new FedUPServerCLI();
        try {
            new picocli.CommandLine(options).parseArgs(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            picocli.CommandLine.usage(options, System.out);
            System.exit(picocli.CommandLine.ExitCode.USAGE);
        }

        if (options.usageHelpRequested) {
            picocli.CommandLine.usage(options, System.out);
            System.exit(CommandLine.ExitCode.OK);
        }

        // TODO create a fedup factory
        //      parse the arg
        //      or encode it within the dataset by default.
        // ModuloOnSuffix strategy = new ModuloOnSuffix(1);

        List<Pair<String, Summary>> summaries = new ArrayList<>();
        for (String pathOrUri : options.summaryPaths) {
            Summary summary;
            if (Path.of(pathOrUri).toFile().isDirectory()) {
                summary = new Summary(new ModuloOnSuffix(1), Location.create(Path.of(pathOrUri)));
            } else {
                summary = new Summary(new ModuloOnSuffix(1));
                summary.setRemote(pathOrUri); // TODO check if actually an URI
            }
            summary.setPattern(options.filterRegex);

            // Export the results in the HTTP response?
            summary.getSummary().getContext().set(FedUPConstants.EXPORT_PLANS, options.export);
            // Which engine use once the sources are assigned?
            switch (options.engine) {
                case "FedX","fedx" -> summary.getSummary().getContext().set(FedUPConstants.EXECUTION_ENGINE, FedUPConstants.FEDX);
                default -> summary.getSummary().getContext().set(FedUPConstants.EXECUTION_ENGINE, FedUPConstants.APACHE_JENA);
            }

            summary.getSummary().getContext().set(ARQ.optimization, false); // make sure no default opti
            String summaryName;
            try {
                // if uri, name is
                URI uri = new URI(pathOrUri);
                summaryName = uri.getAuthority().replace(":", "_").replace(".", "_");
            } catch (NullPointerException | URISyntaxException e) {
                if (Path.of(pathOrUri).toFile().isDirectory()) {
                    summaryName = Path.of(pathOrUri).getFileName().toString();
                } else if (Path.of(pathOrUri).toFile().isFile()) {
                    summaryName = FilenameUtils.getBaseName(pathOrUri);
                } else {
                    System.err.println("Cannot open the summary file: " + pathOrUri);
                    System.exit(picocli.CommandLine.ExitCode.USAGE);
                    return;
                }
            }
            summaries.add(new ImmutablePair<>(summaryName, summary));
            if (Objects.nonNull(options.modifyEndpoints) && !options.modifyEndpoints.isEmpty()) {
                // When graphs in summaries differ from actual endpoints, it's useful to
                // be able to change them at runtime, without re-ingesting the summary.
                Function<String, String> lambda = InMemoryLambdaJavaFileObject.getLambda("ModifyEndpoints",
                        options.modifyEndpoints, "String");
                if (Objects.isNull(lambda)) {
                    System.err.println("The lambda expression does not seem valid.");
                    System.exit(picocli.CommandLine.ExitCode.USAGE);
                }
                summary.getSummary().getContext().set(FedUPConstants.MODIFY_ENDPOINTS, lambda);
            }
        }

        FedUPEngine.register();
        ResultSetLang.init();
        ResultSetReaderRegistry.init();
        RowSetWriterRegistry.register(ResultSetLang.RS_JSON, FedUPPlanAndNormalJSON.factory);

        var builder = FusekiServer.create()
                .port(options.port)
                .enableCors(true, null)
                .verbose(true);

        for (Pair<String, Summary> nameAndSummary : summaries) {
            builder.add(nameAndSummary.getLeft(), nameAndSummary.getRight().getSummary());
            nameAndSummary.getRight().getSummary().getContext().set(FedUPConstants.SUMMARY, nameAndSummary.getRight());
        }

        FusekiServer server = builder.build().start();
        System.out.println("The summaries are available at:");
        for (Pair<String, Summary> nameAndSummary : summaries) {
            System.out.printf("\t- http://localhost:%s/%s/sparql%n", server.getPort(), nameAndSummary.getLeft());
        }

    }

}
