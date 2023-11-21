package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.*;

/**
 * Same as {@link FedQPLVisitor} but with an additional argument
 * @param <T> The return type.
 * @param <S> The argument type.
 */
public interface FedQPLVisitorArg<T, S> {
    default T visit(Mu mu, S arg) {throw new UnsupportedOperationException("Mu");}
    default T visit(Mj mj, S arg) {throw new UnsupportedOperationException("Mj");}
    default T visit(Req req, S arg) {throw new UnsupportedOperationException("Req");}
    default T visit(LeftJoin lj, S arg) {throw new UnsupportedOperationException("LeftJoin");}
    default T visit(Filter filter, S arg) {throw new UnsupportedOperationException("Filter");}
}
