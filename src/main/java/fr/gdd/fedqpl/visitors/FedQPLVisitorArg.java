package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;
import org.apache.jena.sparql.algebra.op.*;

/**
 * Same as {@link FedQPLVisitor} but with an additional argument
 * @param <T> The return type.
 * @param <S> The argument type.
 */
public interface FedQPLVisitorArg<T, S> {
    // operators
    default T visit(Mu mu, S arg) {throw new UnsupportedOperationException("Mu");}
    default T visit(Mj mj, S arg) {throw new UnsupportedOperationException("Mj");}
    default T visit(OpService req, S arg) {throw new UnsupportedOperationException("Req");}
    default T visit(OpLeftJoin lj, S arg) {throw new UnsupportedOperationException("LeftJoin");}
    default T visit(OpFilter filter, S arg) {throw new UnsupportedOperationException("Filter");}

    // query modifiers
    default T visit(OpSlice limit, S arg) {throw new UnsupportedOperationException("Limit");}
    default T visit(OpOrder orderBy, S arg) {throw new UnsupportedOperationException("OrderBy");}
    default T visit(OpProject project, S arg) {throw new UnsupportedOperationException("Project");}
}
