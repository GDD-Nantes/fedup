package fr.gdd.fedup.summary;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A simple in memory summary to help with testing.
 */
public class InMemorySummaryFactory {

    static Dataset petsDataset;

    /**
     * /!\ must call summary first (TODO) remove this requirement
     */
    public static Dataset getPetsDataset() {
        return petsDataset;
    }

    public static Summary getSimplePetsSummary() {
        Summary summary = SummaryFactory.createModuloOnSuffix(1);
        Dataset dataset = TDB2Factory.createDataset();
        dataset.begin(ReadWrite.WRITE);

        List<String> statements = Arrays.asList(
                "<http://auth/person> <http://auth/named> <http://auth/Alice>.",
                "<http://auth/person> <http://auth/named> <http://auth/Bob>.",
                "<http://auth/Alice>  <http://auth/owns>  <http://auth/cat>."
        );

        InputStream statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelA = ModelFactory.createDefaultModel();
        modelA.read(statementsStream, "", Lang.NT.getLabel());

        statements = Arrays.asList(
                "<http://auth/person> <http://auth/named> <http://auth/Carol>.",
                "<http://auth/person> <http://auth/named> <http://auth/David>.",
                "<http://auth/David>  <http://auth/owns>  <http://auth/dog>.",
                "<http://auth/David>  <http://auth/nbPets> \"2\"."
        );
        statementsStream = new ByteArrayInputStream(String.join("\n", statements).getBytes());
        Model modelB = ModelFactory.createDefaultModel();
        modelB.read(statementsStream, "", Lang.NT.getLabel());

        dataset.addNamedModel("https://graphA.org", modelA);
        dataset.addNamedModel("https://graphB.org", modelB);

        dataset.commit();
        dataset.end();
        dataset.begin(ReadWrite.READ);
        Iterator<Quad> quads = dataset.asDatasetGraph().find();

        while (quads.hasNext()) {
            summary.add(quads.next());
        }

        dataset.end();

        petsDataset = dataset;

        return summary;
    }
}
