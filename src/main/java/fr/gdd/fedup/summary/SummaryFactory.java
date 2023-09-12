package fr.gdd.fedup.summary;

import fr.gdd.fedup.summary.strategies.ModuloOnSuffix;
import fr.gdd.fedup.summary.strategies.ModuloOnWhole;
import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.TransformCopy;

/**
 * Convenience factory that return the type of summary needed. For more detailed information
 * on summaries, check out the `Transform` in use.
 */
public class SummaryFactory {

    public static Summary createModuloOnSuffix(Integer modulo, Location... location) {
        return new Summary(new ModuloOnSuffix(modulo), location);
    }

    public static Summary createModuloOnWhole(Integer modulo, Location... location) {
        return new Summary(new ModuloOnWhole(modulo), location);
    }

    public static Summary createIdentity(Location... location) {
        return new Summary(new TransformCopy(), location);
    }

}
