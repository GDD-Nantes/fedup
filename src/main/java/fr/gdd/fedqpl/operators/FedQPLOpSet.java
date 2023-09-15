package fr.gdd.fedqpl.operators;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

public class FedQPLOpSet extends FedQPLOperator implements Set<FedQPLOperator> {

    private Set<FedQPLOperator> ops;

    public FedQPLOpSet() {
        this.ops = new HashSet<>();
    }

    public FedQPLOpSet(Set<FedQPLOperator> ops) {
        this.ops = ops;
    }

    @Override
    public void visit(OpVisitor opVisitor) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'visit'");
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return "FedQPLOpSet";
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'hashCode'");
    }

    @Override
    public boolean equalTo(Op other, NodeIsomorphismMap labelMap) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'equalTo'");
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return this.ops.size();
    }

    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return this.ops.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return this.ops.contains(o);
    }

    @Override
    public Iterator<FedQPLOperator> iterator() {
        // TODO Auto-generated method stub
        return this.ops.iterator();
    }

    @Override
    public Object[] toArray() {
        // TODO Auto-generated method stub
        return this.ops.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        // TODO Auto-generated method stub
        return this.ops.toArray(a);
    }

    @Override
    public boolean add(FedQPLOperator e) {
        // TODO Auto-generated method stub
        return this.ops.add(e);
    }

    @Override
    public boolean remove(Object o) {
        // TODO Auto-generated method stub
       return this.ops.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return this.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends FedQPLOperator> c) {
        // TODO Auto-generated method stub
        return this.containsAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return this.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return this.removeAll(c);
    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub
        this.ops.clear();
    }
    
}
