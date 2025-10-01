package fr.gdd.fedup.summary;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.tdb2.sys.TDBInternal;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A simple in memory summary to help with testing.
 */
public class InMemorySummaryFactory implements AutoCloseable {

    Dataset petsDataset;
    Summary simplePetsSummary;

    /**
     * @return A dataset comprising triples about pets and ownership distributed
     *         in two graphs.
     */
    public Dataset getPetsDataset() {
        if (Objects.isNull(petsDataset)) {
            getSimplePetsSummary();
        }
        return petsDataset;
    }

    /**
     * @return The summary dataset of the dataset about pets.
     */
    public Summary getSimplePetsSummary() {
        if (Objects.nonNull(simplePetsSummary)) {
            return simplePetsSummary;
        }
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
                "<http://auth/David>  <http://auth/nbPets> \"2\".",
                "<http://auth/dog>    <http://auth/family> <http://auth/canid>."
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

        dataset.commit();
        dataset.end();

        petsDataset = dataset;
        simplePetsSummary = summary;

        return simplePetsSummary;
    }

    public Dataset getGraph(String name) {
        getSimplePetsSummary();
        Model modelA = petsDataset.getNamedModel(name);
        Dataset dataset = TDB2Factory.createDataset();
        petsDataset.begin(ReadWrite.READ);
        dataset.begin(ReadWrite.WRITE);
        dataset.setDefaultModel(modelA);
        dataset.commit();
        dataset.close();
        petsDataset.commit();
        petsDataset.close();
        return dataset;
    }

    @Override
    public void close() {
        if (Objects.nonNull(petsDataset)) {
            if (petsDataset.isInTransaction()) petsDataset.commit();
            petsDataset.close();
            TDBInternal.expel(petsDataset.asDatasetGraph());
            petsDataset = null;
        }
        if (Objects.nonNull(simplePetsSummary)) {
            if (simplePetsSummary.getSummary().isInTransaction()) simplePetsSummary.getSummary().commit();
            simplePetsSummary.getSummary().close();
            TDBInternal.expel(simplePetsSummary.getSummary().asDatasetGraph());
            simplePetsSummary = null;
        }
    }
}
