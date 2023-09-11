package fr.gdd.fedup.summary;

import com.github.jsonldjava.utils.Obj;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.tdb2.TDB2Factory;

import java.util.Objects;

public class HashGraphSummarizer {

    Dataset summary;
    Integer modulo;

    public HashGraphSummarizer(Integer modulo, Location... location) {
        this.modulo = modulo;
        if (Objects.nonNull(location) && location.length > 0) {
            summary = TDB2Factory.connectDataset(location[0]);
        } else {
            summary = TDB2Factory.createDataset();
        }
    }

    public void add(Quad quad) {
        summary.begin(TxnType.WRITE);
        Quad toAdd = HashSummarizer.summarize(quad, modulo);
        Model model = summary.getNamedModel(toAdd.getGraph().getURI());
        model.add(model.asStatement(toAdd.asTriple()));
        summary.commit();
        summary.end();
    }

    public Dataset getSummary() {
        return summary;
    }
}
