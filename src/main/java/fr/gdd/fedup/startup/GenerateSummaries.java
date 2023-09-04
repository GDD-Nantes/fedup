package fr.gdd.fedup.startup;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.aksw.simba.quetsal.util.HibiscusSummariesGenerator;
import org.aksw.simba.quetsal.util.TBSSSummariesGenerator;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.jena.atlas.io.IndentedLineBuffer;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.iterator.IteratorCloseable;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.out.NodeFormatterNT;
import org.apache.jena.riot.system.AsyncParser;
import org.apache.jena.sparql.core.Quad;
import org.apache.log4j.Logger;

import fr.univnantes.gdd.fedup.summary.HashSummarizer;

public class GenerateSummaries {

    private static Logger logger = Logger.getLogger(GenerateSummaries.class);

    private static List<String> loadEndpoints(String filename) throws IOException {
        Path filePath = Path.of(filename);
        String endpoints = Files.readString(filePath);
        return Arrays.asList(endpoints.split("\n"));
    }

    private static String loadQuery(String filename) throws IOException {
        Path filePath = Path.of(filename);
        return Files.readString(filePath);
    }

    private static void writeQuery(String queryString, String filename) throws IOException {
        Path filePath = Path.of(filename);
        Files.writeString(filePath, queryString);
    }

    private static String rewriteQuery(String queryString, int modulo) throws Exception {
        Query query = QueryFactory.create(queryString);
        Query newQuery = new HashSummarizer(modulo).summarize(query);
        logger.info("oldQuery: " + query.toString());
        logger.info("newQuery: " + newQuery.toString());
        return newQuery.toString();
    }

    private static void summarizeDataset(String filename, String output, int modulo) throws IOException {
        File file = new File(output);
        FileWriter fileWriter = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        IteratorCloseable<Quad> iter = AsyncParser.asyncParseQuads(filename);
        HashSummarizer summarizer = new HashSummarizer(modulo);
        NodeFormatterNT formatterNT = new NodeFormatterNT();
        Set<Integer> seen = new TreeSet<>();
        Integer[] num_quads = {0};
        iter.forEachRemaining(quad -> {
            try {
                Quad newQuad = summarizer.summarize(quad);
                int hashcode = newQuad.toString().hashCode();
                if (!seen.contains(hashcode)) {
                    IndentedWriter writer = new IndentedLineBuffer(); 
                    formatterNT.format(writer, newQuad.getSubject());
                    writer.print(" ");
                    formatterNT.format(writer, newQuad.getPredicate());
                    writer.print(" ");
                    formatterNT.format(writer, newQuad.getObject());
                    writer.print(" ");
                    formatterNT.format(writer, newQuad.getGraph());
                    writer.println(".");
                    bufferedWriter.write(writer.toString());
                    writer.close();
                    seen.add(hashcode);
                }
                num_quads[0] += 1;
                if (num_quads[0] % 1000000 == 0) {
                    System.out.println("Num Quads: " + num_quads[0]);
                }
            } catch (IOException e) {
                logger.warn(quad + " has been rejected because an error occured: " + e);
            } 
        });
        bufferedWriter.flush();
        bufferedWriter.close();
        fileWriter.close();
    }

    private static void hibiscus(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoints", true, "list of endpoints");
        options.addOption("o", "output", true, "summary output file");
        
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        List<String> endpoints;
        if (cmd.hasOption("endpoints")) {
            endpoints = loadEndpoints(cmd.getOptionValue("endpoints"));
        } else {
            throw new Exception("No endpoints provided");
        }

        String output;
        if (cmd.hasOption("output")) {
            output = cmd.getOptionValue("output");
        } else {
            throw new Exception("No output file specified");
        }

        HibiscusSummariesGenerator generator = new HibiscusSummariesGenerator(output);
        generator.generateSummaries(endpoints, null);
    }

    private static void costfed(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("e", "endpoints", true, "list of endpoints");
        options.addOption("b", "branching-factor", true, "costfed summaries parameter");
        options.addOption("o", "output", true, "summary output file");
        
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        List<String> endpoints;
        if (cmd.hasOption("endpoints")) {
            endpoints = loadEndpoints(cmd.getOptionValue("endpoints"));
        } else {
            throw new Exception("No endpoints provided");
        }

        int branchLimit;
        if (cmd.hasOption("branching-factor")) {
            branchLimit = Integer.parseInt(cmd.getOptionValue("branching-factor"));
        } else {
            branchLimit = 4; // used in the CostFed paper
        }

        String output;
        if (cmd.hasOption("output")) {
            output = cmd.getOptionValue("output");
        } else {
            throw new Exception("No output file specified");
        }

        TBSSSummariesGenerator generator = new TBSSSummariesGenerator(output);
        generator.generateSummaries(endpoints, null, branchLimit);
    }

    private static void fedup(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("f", "file", true, "SPARQL query");
        options.addOption("q", "query", true, "SPARQL query");
        options.addOption("d", "dataset", true, "RDF dataset");
        options.addOption("m", "modulo", true, "compactness of the summary");
        options.addOption("o", "output", true, "summary output file");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        int modulo;
        if (cmd.hasOption("modulo")) {
            modulo = Integer.parseInt(cmd.getOptionValue("modulo"));
        } else {
            modulo = 0; // used in the CostFed paper
        }

        String output;
        if (cmd.hasOption("output")) {
            output = cmd.getOptionValue("output");
        } else {
            throw new Exception("No output file specified");
        }

        if (cmd.hasOption("query")) {
            String queryString = loadQuery(cmd.getOptionValue("query"));
            writeQuery(rewriteQuery(queryString, modulo), output);
        } else if (cmd.hasOption("file")) {
            String queryString = loadQuery(cmd.getOptionValue("file"));
            writeQuery(rewriteQuery(queryString, modulo), output);
        } else if (cmd.hasOption("dataset")) {
            summarizeDataset(cmd.getOptionValue("dataset"), output, modulo);
        } else {
            throw new Exception("No SPARQL query or dataset provided");
        }
    } 
    
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new Exception("No command specified");
        } else if (args[0].equals("hibiscus")) {
            hibiscus(args);
        } else if (args[0].equals("costfed")) {
            costfed(args);
        } else if (args[0].equals("fedup")) {
            fedup(args);
        } else {
            throw new Exception("Unknown command '" + args[0] + "'");
        }
    }
}
