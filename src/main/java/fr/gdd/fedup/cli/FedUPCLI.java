package fr.gdd.fedup.cli;

import fr.gdd.fedup.FedUP;
import fr.gdd.fedup.summary.ModuloOnSuffix;
import fr.gdd.fedup.summary.Summary;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.resultset.ResultSetReaderRegistry;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.engine.QueryIterator;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

/**
 * FedUP that runs as a command line interface, as opposed to the server run. This allows
 * easy retrieving of source selection plans.
 */
@picocli.CommandLine.Command(
        name = "fedup",
        version = "0.1.0",
        description = "Federation engine for SPARQL query processing.",
        usageHelpAutoWidth = true, // adapt to the screen size instead of new line on 80 chars
        sortOptions = false,
        sortSynopsis = false,
        mixinStandardHelpOptions = true
)
public class FedUPCLI {

    static class ExclusiveQuery {
        @picocli.CommandLine.Option(
                order = 2,
                names = {"-q", "--query"},
                paramLabel = "SPARQL",
                description = "The SPARQL query to execute.")
        String queryAsString;

        @picocli.CommandLine.Option(
                order = 2,
                names = {"-f", "--file"},
                paramLabel = "path/to/query",
                description = "The file containing the SPARQL query to execute.")
        String queryFile;
    }

    @CommandLine.ArgGroup(multiplicity = "1")
    ExclusiveQuery exclusiveQuery;

    @picocli.CommandLine.Option(
            order = 3,
            names = {"-s", "--summary"},
            paramLabel = "path/to/tdb2|http://endpoint/sparql",
            description = """
                    Path to the summary dataset. The path is either local and targets an \
                    Apache Jena's TDB2 dataset folder; or a remote SPARQL endpoint hosting the \
                    summary.""")
    String summaryPath;

    // options.addOption("t", "type", true, "The summary type (example: ModuloOnSuffix(1)).");

    @picocli.CommandLine.Option(
            order = 4,
            names = {"-e", "--engine"},
            paramLabel = "Jena|FedX",
            description = """
                    The federation engine in charge of the executing the SPARQL query with SERVICE clauses. \
                    When the engine is set to None, the query is not executed, but the source selection is still \
                    performed: this can facilitate debugging. Default: ${DEFAULT-VALUE}""")
    String engine = "None";

    @picocli.CommandLine.Option(
            order = 5,
            names = {"-x", "--explain"},
            description = """
                    Prints some details about execution times; and the source selection plan, \
                    i.e., the logical plan with SERVICE clauses designating the chosen sources.""")
    Boolean explain = false;


    @picocli.CommandLine.Option(
            order = 6,
            names = {"-m", "--modify"},
            paramLabel = "λ-expr",
            description = """
                    Java lambda expression to apply to graphs in summaries in order to call actual endpoints. \
                    Therefore, even if the \
                    sources of summarized triples diverge from the actual serving endpoint, \
                    this bridges the difference. Default: ${DEFAULT-VALUE}""")
    String modifyEndpoints = "(e) -> \"http://localhost:5555/sparql?default-graph-uri=\"+(e.substring(0, e.length() - 1))";

    @picocli.CommandLine.Option(
            order = 7,
            names = {"--filter"},
            paramLabel = "regex",
            description = """
                The summary may contain more graphs than necessary. This allows filtering, to keep only the graphs \
                that are of interest. Default: ${DEFAULT-VALUE}""")
    public String filterRegex = ".*"; // by default allows everything

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
            System.err.println(e.getMessage());
            picocli.CommandLine.usage(options, System.out);
            System.exit(picocli.CommandLine.ExitCode.USAGE);
        }
        if (options.usageHelpRequested) {
            picocli.CommandLine.usage(options, System.out);
            System.exit(CommandLine.ExitCode.OK);
        }

        if (Objects.nonNull(options.exclusiveQuery.queryFile)) {
            Path queryPath = Path.of(options.exclusiveQuery.queryFile);
            try {
                options.exclusiveQuery.queryAsString = Files.readString(queryPath);
            } catch (IOException e) {
                System.out.println("Error: could not read " + queryPath.toString() + ".");
                System.exit(CommandLine.ExitCode.SOFTWARE);
            }
        }

        if (options.explain) {
            System.err.println(options.exclusiveQuery.queryAsString);
        }

        // important to initialize these two now or else they might not be
        // initialized when ASK queries are performed… It cannot accept nor parse the
        // received results.
        ResultSetLang.init();
        ResultSetReaderRegistry.init();

        // TODO, no necessarily modulo on suffix…
        Summary summary;
        if (Path.of(options.summaryPath).toFile().isDirectory()) {
            summary = new Summary(new ModuloOnSuffix(1), Location.create(Path.of(options.summaryPath)));
        } else {
            summary = new Summary(new ModuloOnSuffix(1));
            summary.setRemote(options.summaryPath); // TODO check if actually an URI
        }
        summary.setPattern(options.filterRegex);

        long startTimeGraphs = System.currentTimeMillis();
        FedUP fedup = new FedUP(summary);
        long elapsedTimeGraphs = System.currentTimeMillis() - startTimeGraphs;
        if (options.explain) {
            // because graphs are lazily initialized in fedup.
            System.err.printf("Took %s ms to get the federation members.%n", elapsedTimeGraphs);
        }

        if (Objects.nonNull(options.modifyEndpoints) && !Objects.equals(options.modifyEndpoints, "")) {
            Function<String, String> lambda =
                    InMemoryLambdaJavaFileObject.getLambda("ModifyEndpoints",
                    options.modifyEndpoints, "String");
            if (Objects.isNull(lambda)) {
                throw new UnsupportedOperationException("The lambda expression does not seem valid.");
            }
            fedup.modifyEndpoints(lambda);
        }

        long parseStart  =System.currentTimeMillis();
        Op query = Algebra.compile(QueryFactory.create(options.exclusiveQuery.queryAsString));
        long parseElapsed = System.currentTimeMillis() - parseStart;

        if (options.explain) {
            System.err.printf("Took %s ms to parse the query.%n", parseElapsed);
        }

        long sourceSelectionStart = System.currentTimeMillis();
        Pair<TupleExpr, Op> both = fedup.queryJenaToBothFedXAndJena(query);
        long sourceAssignmentElapsed = System.currentTimeMillis() - sourceSelectionStart;

        if (options.explain) {
            Op queryWithSources = both.getRight();
            if (Objects.nonNull(queryWithSources)) {
                System.err.println(OpAsQuery.asQuery(queryWithSources).toString());
            }
            System.err.printf("Took %s ms to perform the source assignment.%n", sourceAssignmentElapsed);
        }

        if (Objects.isNull(options.engine)) {
            System.exit(CommandLine.ExitCode.OK);
        }


        long executionStart = System.currentTimeMillis();
        QueryIterator results = switch (options.engine) {
            case "Jena", "jena" -> fedup.executeWithJena(both.getRight());
            case "FedX", "fedx" -> fedup.executeWithFedX(both.getLeft());
            default -> null; // nothing
        };

        if (Objects.isNull(results)) {
            System.exit(CommandLine.ExitCode.OK);
        }

        long nbResults = 0L;
        while (results.hasNext()) {
            System.out.println(results.next());
            nbResults += 1;
        }

        long elapsedExecution = System.currentTimeMillis() - executionStart;
        if (options.explain) {
            System.err.printf("Took %s ms to retrieve %s mappings.%n", elapsedExecution, nbResults);
        }

        System.exit(CommandLine.ExitCode.OK);
    }

}
