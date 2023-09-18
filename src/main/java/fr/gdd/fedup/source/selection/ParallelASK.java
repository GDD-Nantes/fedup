package fr.gdd.fedup.source.selection;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ParallelASK {

    ConcurrentHashMap<ImmutablePair<String, OpTriple>, Boolean> asks = new ConcurrentHashMap<>();
    Set<String> endpoints;
    OpBGP triples;

    public ParallelASK(Set<String> endpoints, OpBGP triples) {
        this.endpoints = endpoints;
        this.triples = triples;
        // TODO use a generic ask performer so it can do it remotely or not (for debug and tests purposes)
    }





}
