package fr.gdd.fedup.summary;

import org.apache.jena.dboe.base.file.Location;
import org.apache.jena.sparql.algebra.TransformCopy;

import java.util.Objects;

/**
 * Convenience factory that return the type of summary needed. For more detailed information
 * on summaries, check out the `Transform` in use.
 */
public class SummaryFactory {

    public static Summary createModuloOnSuffix(Integer modulo, Location... location) {
        if (Objects.nonNull(location) && location.length > 0) {
            return new Summary(new ModuloOnSuffix(modulo), location[0]);
        } else {
            return new Summary(new ModuloOnSuffix(modulo));
        }
    }

    public static Summary createModuloOnWhole(Integer modulo, Location... location) {
        if (Objects.nonNull(location) && location.length > 0) {
            return new Summary(new ModuloOnWhole(modulo), location[0]);
        } else {
            return new Summary(new ModuloOnWhole(modulo));
        }
    }

    public static Summary createIdentity(Location... location) {
        if (Objects.nonNull(location) && location.length > 0) {
            return new Summary(new TransformCopy(), location[0]);
        } else {
            return new Summary(new TransformCopy());
        }
    }

}
