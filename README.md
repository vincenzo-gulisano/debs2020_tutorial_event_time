# The Role of Event-Time Order in Data Streaming Analysis – Examples

Slides and code from the [ACM DEBS 2020](https://2020.debs.org/) Tutorial titled *"The Role of Event-Time Order in Data Streaming Analysis"*, given by Vincenzo Gulisano (Chalmers University of Technology), Dimitris Palyvos-Giannas (Chalmers University of Technology), Bastian Havers (Chalmers University of Technology & Volvo Cars) and Marina Papatriantafilou (Chalmers University of Technology).

The corresponding paper is available [online](https://2020.debs.org/pdf/debs20-1002.pdf).

The recording of this tutorial can be found at:
https://youtu.be/SW_WS6ULsdY
https://youtu.be/bq3ECNvPwOU

# Setup

## Flink

Download the latest version of Apache Flink [here](https://flink.apache.org/downloads.html) and follow the [Flink DataStream start-up instructions](https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/walkthroughs/datastream_api.html).


## Datasets

The demonstration dataset used for the disorder examples from `DisorderQuery.java` is already provided in the folder `input`.
The larger dataset used in FullQuery is available for download [here](https://chalmersuniversity.box.com/shared/static/33g1669kj3bszn70bz6fs0l1bijejxyu.txt). 
Place it in a location of your choice and change the variable `PATH_TO_LR_SOURCE_DATA` in `Config.java` to the downloaded file. Then, in the same java file, point `PATH_TO_REPOSITORY` to the location to which you downloaded the repository.

## Optional: Grafana dashboard

To your Flink config file found at `YOUR_FLINK_HOME/conf/flink-conf.yaml`, append the following lines:

```yaml
metrics.reporter.grph.class: org.apache.flink.metrics.graphite.GraphiteReporter
metrics.reporter.grph.host: localhost
metrics.reporter.grph.port: 2003
metrics.reporter.grph.protocol: TCP
```
Then, copy `YOUR_FLINK_HOME/opt/flink-metrics-graphite-1.10.0.jar` into `YOUR_FLINK_HOME/lib`.

Then, install Graphite, e.g. as shown [here](https://graphite.readthedocs.io/en/latest/install.html). Flink will send metrics via port 2003 to Graphite.
Finally, get Grafana from [here](https://grafana.com/get) and [connect Graphite to Grafana](https://grafana.com/docs/grafana/latest/features/datasources/graphite/).

Start a Flink cluster (as described [here](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/cluster_setup.html)), and it will show up as a metric in the Graphite connector in Grafana, along with all the Flink jobs that you submit to the cluster.


# Run

Build the jar file with Maven:
```shell script
cd PATH_TO_REPOSITORY
mvn clean package
```

There are two main classes provided in this repository, `DisorderQuery` and `FullQuery`. 

Both have been tested with Apache Flink version >= 1.9.2.

- `DisorderQuery.java` runs without parameters, output files are created in the folder `outputs/disorder`.
- `FullQuery.java` expects two parameters. The first is an integer >= 1 and defines the parallelism of the Join. The second parameter is either `true` or `false`.
If set to `false`, the query will execute normally. `true` enables sleep timers in the operators and stalls the watermark progress slightly, to make differences in the watermarks more visible and to simulate a higher load. Output files are created in the folder `outputs/full`.

An example:

```shell script
./YOUR_FLINK_HOME/bin/flink start
./YOUR_FLINK_HOME/bin/flink run -c DEBS2020streamingEventTime.FullQuery PATH_TO_REPOSITORY/target/DEBS2020streamingEventTime-1.0.jar 4 false
```
will start the Full Query with Join parallelism 4 and no sleep timers.
