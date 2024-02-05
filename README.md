# FedUP

FedUP [1] is a federated SPARQL query engine that priories
unions-over-joins logical plans to efficiently execute federated
queries over a federation of SPARQL endpoints. This repository
provides the code for both the library and a
[Fuseki](https://jena.apache.org/documentation/fuseki2/index.html)
server.


## Installation And Usage

```sh
git clone https://git@github.com:GDD-Nantes/fedup.git
cd fedup
```

```sh
mvn exec:java

# usage: fedup [options] --sumaries <path>
#  -e,--engine <arg>      The federation engine in charge of executing (default: Jena; FedX).
#  -h,--help              print this message
#  -p,--port <arg>        The port of this FedUP server (default: 3330).
#  -s,--summaries <arg>   Path(s) to TDB2 dataset summary(ies).
#  -x,--export            The federated query plan is exported within HTTP responses (default: false).
```

```sh
# As an example, from the Fediscount use case that comprises 3 summaries
 mvn exec:java -Dexec.args="--summaries=./fedshop100-h0,./fedshop20-h0,./fedshop200-h0 --engine=FedX --export"
```

## How Does It Work?

FedUP builds a tiny quotient summary that represents the federation of
SPARQL endpoints. Using the initial query, this summary plus `ASK`
SPARQL queries, FedUP is able to [iteratively build its logical
plan](https://github.com/GDD-Nantes/fedup/blob/main/src/main/java/fr/gdd/fedqpl/SA2FedQPL.java).
Contrarily to the state-of-the-art federation engines that build
joins-over-unions logical plans [2], FedUP builds plan that resembles
unions-over-joins. These plans better preserve the relationship
between triple patterns that actually contribute to the final result.


For detailed information about FedUP's operation, please refer to the
paper [1]. 

## References

[1] Julien Aimonier-Davat, Minh-Hoang Dang, Pascal Molli, Brice
Nédelec, and Hala Skaf-Molli. _FedUP: Querying Large-Scale Federations
of SPARQL Endpoints._ In The Web Conference 2024 (WWW’2024). Singapore, 2024.

[2] Sijin Cheng and Olaf Hartig. _FedQPL: A Language for Logical Query
Plans over Heterogeneous Federations of RDF Data Sources._ In the 22nd
International Conference on Information Integration and Web-Based
Applications & Services. Association for Computing Machinery, 2021.
