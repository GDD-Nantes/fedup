package fr.gdd.fedup.cli;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.fuseki.FedUPConstants;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

/**
 * FedUP that runs as a command line interface, as opposed to the server run. This allows
 * easy retrieving of source selection plans.
 */
@picocli.CommandLine.Command(
        name = "fedup",
        version = "0.0.1",
        description = "Federation engine for SPARQL query processing.",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false
)
public class FedUPCLI {

    @picocli.CommandLine.Option(
            order = 2,
            names = {"-q", "--query"},
            paramLabel = "<SPARQL>",
            description = "The SPARQL query to execute.")
    String queryAsString;

    @picocli.CommandLine.Option(
            order = 2,
            names = {"-f", "--file"},
            paramLabel = "<path/to/query>",
            description = "The file containing the SPARQL query to execute.")
    String queryFile;

    @picocli.CommandLine.Option(
            order = 3,
            names = {"-s", "--summary"},
            paramLabel = "<path/to/TDB2>",
            description = "Path to the TDB2 dataset summary.")
    String summaryPath;

    // options.addOption("t", "type", true, "The summary type (example: ModuloOnSuffix(1)).");

    @picocli.CommandLine.Option(
            order = 4,
            names = {"-e", "--engine"},
            paramLabel = "None | Jena | FedX",
            description = "The federation engine in charge of executing (default: None).")
    String engine;

    @picocli.CommandLine.Option(
            order = 5,
            names = {"-x", "--explain"},
            description = "Prints the source selection plan (default: false).")
    Boolean explain;


    @picocli.CommandLine.Option(
            order = 6,
            names = {"-m", "--modify"},
            paramLabel = "lambda expression",
            description = "Lambda expression to apply to graphs in summaries in order to call actual endpoints.")
    String modifyEndpoints;

    @picocli.CommandLine.Option(
            order = Integer.MAX_VALUE, // last
            names = {"-h", "--help"},
            usageHelp = true,
            description = "Display this help message.")
    boolean usageHelpRequested;

    /* ******************************************************************************** */

    public static void main(String[] args) throws ParseException {
        FedUPCLI options = new FedUPCLI();
        try {
            new picocli.CommandLine(options).parseArgs(args);
        } catch (Exception e) {
            picocli.CommandLine.usage(options, System.out);
            System.exit(picocli.CommandLine.ExitCode.USAGE);
        }

        if (options.usageHelpRequested ||
                (Objects.isNull(options.queryAsString) && Objects.isNull(options.queryFile)) ||
                (Objects.isNull(options.summaryPath))
        ) {
            picocli.CommandLine.usage(options, System.out);
            System.exit(picocli.CommandLine.ExitCode.USAGE);
        }

        if (Objects.nonNull(options.queryFile)) {
            Path queryPath = Path.of(options.queryFile);
            try {
                options.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.out.println("Error: could not read " + queryPath.toString() + ".");
                System.exit(CommandLine.ExitCode.SOFTWARE);
            }
        }


        // TODO, no necessarily modulo on suffix…
        Summary summary = new Summary(new ModuloOnSuffix(1), Location.create(Path.of(options.summaryPath)));

        FedUP fedup = new FedUP(summary);

        if (Objects.nonNull(options.modifyEndpoints) && !Objects.equals(options.modifyEndpoints, "")) {
            Function<String, String> lambda =
                    InMemoryLambdaJavaFileObject.getLambda("ModifyEndpoints",
                    options.modifyEndpoints, "String");
            if (Objects.isNull(lambda)) {
                throw new UnsupportedOperationException("The lambda expression does not seem valid.");
            }
            fedup.modifyEndpoints(lambda);
        }

        Pair<TupleExpr, Op> both = fedup.queryJenaToBothFedXAndJena(Algebra.compile(QueryFactory.create(options.queryAsString)));

        if (options.explain) {
            System.err.println(OpAsQuery.asQuery(both.getRight()).toString());
        }

        switch (options.engine) {
            case "Jena", "jena" -> System.out.println("jena");
            case "FedX", "fedx" -> System.out.println("fedx");
            default -> {} // nothing
        }

    }

}
