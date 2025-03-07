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

```sh
java -jar target/fedup-server.jar

# usage: fedup-server [options] --sumaries <path>
# -e,--engine <arg>      The federation engine in charge of executing (default: Jena; FedX).
# -h,--help              print this message
# -m,--modify <arg>      Lambda expression to apply to graphs in summaries in order to call actual endpoints.
# -p,--port <arg>        The port of this FedUP server (default: 3330).
# -s,--summaries <arg>   Path(s) to TDB2 dataset summary(ies).
# -x,--export            The federated query plan is exported within HTTP responses (default: false).
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
> Usage: summarizer -i=[path/to/tdb2 | http://input/endpoint] -o=[path/to/tdb2 | http://output/endpoint] [-u=<$USERNAME>] [-p=<$PASSWORD>] [--hash=0] [--filter=.*]
> Creates the summary for FedUP.
>  -i, --input=[path/to/tdb2 | http://input/endpoint] Set the dataset to summarize.
>  -o, --output=[path/to/tdb2 | http://output/endpoint] Set the output summary dataset.
>  -u, --username=<$USERNAME> (Not tested) The username for the summary database if needed.
>  -p, --password=<$PASSWORD> (Not tested) The password for the summary database if needed.
>      --hash=0  The modulo value of the hash that summarizes (default: 0).
>      --filter=.*  The regular expression to filter out read graphs.
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
> Usage: fedup [-xh] [-q=<SPARQL>] [-f=<path/to/query>] [-s=<path/to/TDB2>] [-e=None | Jena | FedX] [-m=(e) -> "http://localhost:5555/sparql?default-graph-uri="+(e.substring(0, e.length() - 1))] [--filter=.*]
> Federation engine for SPARQL query processing.
>  -q, --query=<SPARQL>   The SPARQL query to execute.
>  -f, --file=<path/to/query>   The file containing the SPARQL query to execute.
>  -s, --summary=<path/to/TDB2>   Path to the TDB2 dataset summary.
>  -e, --engine=None | Jena | FedX   The federation engine in charge of executing (default: None).
>  -x, --explain          Prints the source selection plan (default: false).
>  -m, --modify=(e) -> "http://localhost:5555/sparql?default-graph-uri="+(e.substring(0, e.length() - 1))
>                      Lambda expression to apply to graphs in summaries in order to call actual endpoints.
>      --filter=.*     The regular expression to filter out read endpoints.
>  -h, --help          Display this help message.
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
