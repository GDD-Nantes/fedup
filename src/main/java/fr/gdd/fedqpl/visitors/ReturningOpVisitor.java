package fr.gdd.fedqpl.visitors;

import fr.gdd.fedqpl.operators.Mj;
import fr.gdd.fedqpl.operators.Mu;
import org.apache.jena.sparql.algebra.op.*;

/**
 * The visitor must implement this interface. Added value compared
 * to the default visitor {@link org.apache.jena.sparql.algebra.OpVisitor}:
 * it returns a type. Remember to use the {@link ReturningOpVisitorRouter} to call
 * downstream visitors.
 * @param <T> The type of the object returned.
 */
public class ReturningOpVisitor<T> {
    public T visit(Mu mu) {throw new UnsupportedOperationException("Mu");}
    public T visit(Mj mj) {throw new UnsupportedOperationException("Mj");}
    public T visit(OpService req) {throw new UnsupportedOperationException("Req");}

    public T visit(OpTriple triple) {throw new UnsupportedOperationException("OpTriple");}
    public T visit(OpQuad quad) {throw new UnsupportedOperationException("OpQuad");}
    public T visit(OpBGP bgp) {throw new UnsupportedOperationException("OpBGP");}
    public T visit(OpSequence sequence) {throw new UnsupportedOperationException("OpSequence");}
    public T visit(OpTable table) {throw new UnsupportedOperationException("OpTable");}
    public T visit(OpLeftJoin lj) {throw new UnsupportedOperationException("OpLeftJoin");}
    public T visit(OpConditional cond) {throw new UnsupportedOperationException("OpConditional");}
    public T visit(OpFilter filter) {throw new UnsupportedOperationException("OpFilter");}
    public T visit(OpUnion union) {throw new UnsupportedOperationException("OpUnion");}
    public T visit(OpJoin join) {throw new UnsupportedOperationException("OpJoin");}

    public T visit(OpDistinct distinct) {throw new UnsupportedOperationException("OpDistinct");}
    public T visit(OpSlice slice) {throw new UnsupportedOperationException("OpSlice");}
    public T visit(OpOrder orderBy)  {throw new UnsupportedOperationException("OpOrder");}
    public T visit(OpProject project) {throw new UnsupportedOperationException("OpProject");}
    public T visit(OpGroup groupBy) {throw new UnsupportedOperationException("OpGroup");}
}
