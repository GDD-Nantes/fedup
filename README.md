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
mvn clean package
```

```
Usage: fedup-server [-xh] [-p=3330] -s=path/to/tdb2|http://output/endpoint[,path/to/tdb2|http:
                    //output/endpoint...] [-s=path/to/tdb2|http://output/endpoint[,path/to/tdb2|http:
                    //output/endpoint...]]... -e=Jena|FedX [-m=λ-expr] [--filter=regex]
Federation engine as a server for SPARQL query processing.
  -p, --port=3330          The port of this FedUP server. Default: 3330
  -s, --summaries=path/to/tdb2|http://output/endpoint[,path/to/tdb2|http://output/endpoint...]
                           Path to the summary datasets. Each path is either local and targets an
                             Apache Jena's TDB2 dataset folder; or a remote SPARQL endpoint hosting
                             the summary.
  -e, --engine=Jena|FedX   The federation engine in charge of the executing the SPARQL query with
                             SERVICE clauses. When the engine is set to None, the query is not
                             executed, but the source selection is still performed: this can
                             facilitate debugging. Default: Jena
  -x, --export             From a SPARQL query, FedUP creates a federated query with additional
                             SERVICE clauses. This option exports the federated query plan within the
                             HTTP response. In the JSON response, besides the results bindings, FedUP
                             adds "FedUP_Exported" as a plain text query.
  -m, --modify=λ-expr      Java lambda expression to apply to graphs in summaries in order to call
                             actual endpoints. Therefore, even if the sources of summarized triples
                             diverge from the actual serving endpoint, this bridges the difference.
                             Default: (e) -> "http://localhost:5555/sparql?default-graph-uri="+(e.
                             substring(0, e.length() - 1))
      --filter=regex       The summary may contain more graphs than necessary. This allows filtering,
                             to keep only the graphs that are of interest. Default: .*
  -h, --help               Display this help message.
```

```sh
# As an example, from the Fediscount use case that comprises 3 summaries
java -jar target/fedup-server.jar --summaries=./fedshop100-h0,./fedshop20-h0,./fedshop200-h0 --engine=FedX --export
```

> [!NOTE]
> <details>
> <summary> How to build a summary you ask? </summary>
> <pre>
> java -jar target/summarizer.jar
> </pre>
> <pre>
> Usage: summarizer [-hV] -i=path/to/tdb2|http://input/endpoint -o=path/to/tdb2|http://output/endpoint
>                   [-u=$USERNAME] [-p=$PASSWORD] [--hash=integer] [--filter=regex]
> Creates the summary for FedUP.
>   -h, --help                 Show this help message and exit.
>   -V, --version              Print version information and exit.
>   -i, --input=path/to/tdb2|http://input/endpoint
>                              Path to the summary dataset. The path is either local and targets an
>                                Apache Jena's TDB2 dataset folder; or a remote SPARQL endpoint hosting
>                                the quads.
>   -o, --output=path/to/tdb2|http://output/endpoint
>                              Path to the summary dataset. The path is either local and targets an
>                                Apache Jena's TDB2 dataset folder; or a remote SPARQL endpoint hosting
>                                the summary being built.
>   -u, --username=$USERNAME   (Not tested) The username for the summary database if needed.
>   -p, --password=$PASSWORD   (Not tested) The password for the summary database if needed.
>       --hash=integer         The modulo value of the hash that summarizes. Default: 0
>       --filter=regex         The summary may contain more graphs than necessary. This allows
>                                filtering, to keep only the graphs that are of interest. Default: .*
> </pre>
> In <code>0.0.2</code>, for better interoperability, the summarizer also allows ingesting from
> any kind of remote SPARQL endpoint (although slower than
> using a local TDB2 database) to any kind 
> of remote SPARQL endpoint:
> <pre>
> java -jar target/summarizer.jar \
>    --input=http://localhost:5555/sparql \
>    --output=http://localhost:8080/sparql \
>    --filter="^http://www.vendor.*|^http://www.rating.*"
> </pre>
> Running this summarizer command allowed us to build <b>from</b> Virtuoso's
> FedShop200 endpoint <b>to</b> a Virtuoso summary database. The <code>--filter</code>
> makes sure that we summarize only the graphs of FedShop200. It took 
> roughly 1h since the summarizer needs to download the graphs one by one
> from the input endpoint.
> </details>

> [!TIP]
> <details>
> <summary>Alternatively, you can run FedUP without server, using a command line interface.</summary>
> It provides a convenient mean to retrieve the unions-over-joins logical
> plan with <code>--explain</code>, and then, optionally execute it using <code>-e Jena</code>
> or <code>-e FedX</code>.
> <pre>
> java -jar target/fedup.jar
> </pre>
> <pre>
> Usage: fedup [-xh] [-s= path/to/tdb2|http://endpoint/sparql ] [-e=Jena|FedX] [-m= λ-expr ]
>             [--filter= regex ] (-q= SPARQL  | -f= /to/query )
> Federation engine for SPARQL query processing.
>   -q, --query=SPARQL       The SPARQL query to execute.
>   -f, --file=/to/query     The file containing the SPARQL query to execute.
>   -s, --summary=path/to/tdb2 | http://endpoint/sparql
>                            Path to the summary dataset. The path is either local and targets an
>                              Apache Jena's TDB2 dataset folder; or a remote SPARQL endpoint hosting
>                              the summary.
>   -e, --engine=Jena|FedX   The federation engine in charge of the executing the SPARQL query with
>                              SERVICE clauses. When the engine is set to None, the query is not
>                              executed, but the source selection is still performed: this can
>                              facilitate debugging. Default: None
>   -x, --explain            Prints some details about execution times; and the source selection plan,
>                              i.e., the logical plan with SERVICE clauses designating the chosen
>                              sources.
>   -m, --modify=<λ-expr>    Java lambda expression to apply to graphs in summaries in order to call
>                              actual endpoints. Therefore, even if the sources of summarized triples
>                              diverge from the actual serving endpoint, this bridges the difference.
>                              Default: (e) -> "http://localhost:5555/sparql?default-graph-uri="+(e.
>                              substring(0, e.length() - 1))
>       --filter=regex       The summary may contain more graphs than necessary. This allows filtering,
>                              to keep only the graphs that are of interest. Default: .*
>   -h, --help               Display this help message.
>  </pre>
> </details>


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
