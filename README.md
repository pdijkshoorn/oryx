_See also the experimental [Oryx 2 incubating](https://github.com/OryxProject/oryx) project!_

# Summary

<img align="right" src="https://raw.github.com/wiki/cloudera/oryx/OryxLogoSmall.png"/>

The Oryx open source project provides simple, real-time large-scale machine learning /
predictive analytics infrastructure. It implements a few classes of algorithm commonly used in business applications:
*collaborative filtering / recommendation*, *classification / regression*, and *clustering*.
It can continuously build models from a stream of data at large scale using
[Apache Hadoop](http://hadoop.apache.org/). It also serves queries of those models in real-time via an HTTP
[REST](http://en.wikipedia.org/wiki/Representational_state_transfer) API, and can update
models approximately in response to streaming new data. This two-tier design, comprised of the
Computation Layer and Serving Layer, respectively, implement a
[lambda architecture](http://jameskinley.tumblr.com/post/37398560534/the-lambda-architecture-principles-for-architecting).
Models are exchanged in [PMML](http://www.dmg.org/v4-1/GeneralStructure.html) format.
It is not a library, visualization tool, exploratory analytics tool, or environment.

# Architecture ( just a test)

<img align="right" src="https://raw.github.com/wiki/cloudera/oryx/OryxArchitecture.png" width="481" height="286"/>

Oryx does two things at heart: *builds models*, and *serves models*. These are the responsibilities of two
separate components, the Computation Layer and Serving Layer, respectively.

## Computation Layer

The Computation Layer is an offline, batch process that builds a machine learning model from input data.
Its operation proceeds in "generations", where a model is built from a snapshot of input at a point in time. The
result is a succession of model outputs over time, built from a cumulative succession of inputs.

The Computation Layer is a long-running Java-based server process. It can be used independently of the
Serving Layer to just build models, or even "score" models offline (e.g. produce recommendations for users
offline).

Input arrives on [HDFS](http://wiki.apache.org/hadoop/HDFS) and models are written to HDFS as PMML files.
Input data can be collected by the
Serving Layer, which records input into HDFS, or can be added by other processes directly to HDFS. The
Serving Layer automatically loads new models from HDFS as they become available.

### Distributed

The Computation Layer is primarily intended to use Hadoop's computation environment for computation, which
for the moment means [MapReduce](http://en.wikipedia.org/wiki/MapReduce).
In this "distributed" context, the Computation Layer process is minding a
series of jobs that execute on the cluster. The server process configures, launches and monitors the jobs.
Input, intermediate outputs, and model output are all on HDFS.

### Local

The Computation Layer can also be configured to run the computations locally rather than on Hadoop, and to read
and write data to the local file system. It will simply use a multi-threaded in-memory, non-Hadoop implementation.
This is useful for small or non-critical problems, or for simple testing.

### Directory Layout

All data is stored under one, configured root directory (`model.instance-dir`). Example: `/user/oryx/iris/`.
Under this directory are a series of numbered generation directories: `00000/`, `00001/`, ... Each of these contains
files for one generation. The exact contents of these directories varies by algorithm, but in all cases, input
arrives into an `inbound/` subdirectory, and the model file is generated at `model.pmml.gz` in the
generation directory.

## Serving Layer

The Serving Layer is also a long-running Java-based server process, which exposes a REST API. It can be accessed
from a browser, or any language or tool that can make HTTP requests.

The endpoints that it exposes vary by algorithm. For example, the collaborative filtering implementation will
expose endpoints like `/recommend`, which respond to a `GET` request with particular URL path parameters and
returns recommended items.

Input and output formats vary by endpoint, but are generally simple text or delimited text.

Many Serving Layer instances can be run at once. They act and serve independently of one another. In a cluster
configuration, it is typically most effective to front these Serving Layer instances with a load balancer with
session stickiness enabled.

# Algorithms

## Collaborative filtering / Recommendation

Oryx implements a matrix factorization-based approach based on a variant of
[ALS (alternating least squares)](http://labs.yahoo.com/files/HuKorenVolinsky-ICDM08.pdf) for collaborative filtering /
recommendation. Recommender engines are most popularly used to suggest items like books and movies to people,
but can in general be used to guess unobserved associations between entities given many observed associations.

## Classification and Regression

Oryx supports [random decision forests](http://en.wikipedia.org/wiki/Random_forest) for classification and
regression tasks. This is a form of supervised learning, where a value is predicted for new inputs based on
known values for previous inputs. This includes classification tasks -- predicting a category like "spam" --
and regression tasks -- predicting a numeric value like salary.

## Clustering

Oryx implements [scalable k-means++](http://arxiv.org/abs/1203.6402) for clustering. This is a type of unsupervised
learning, which seeks to find structure in its input in the form of natural groupings.

# Download

Releases of the compiled binaries are available from the [Releases page](https://github.com/cloudera/oryx/releases).

You can always download a current [snapshot of `master` branch](https://github.com/cloudera/oryx/archive/master.zip)
as a `.zip` file.

# [Building from Source](https://github.com/cloudera/oryx/wiki/Building-from-Source)

# [Installation](https://github.com/cloudera/oryx/wiki/Installation)

# Running

## Configuring

To run the Serving Layer or the Computation Layer, you must point to a configuration file.
The complete set of configuration options is defined and documented in `common/src/main/resources/reference.conf`

A configuration file is simply a text file using [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md)
syntax (roughly, a combination of [JSON](http://www.json.org/) and simple properties file syntax).

Oryx obtains its configuration from four top-level objects:

* `model`: required. Specifies the type of model (e.g., ALS or K-means)
* `inbound`: optional. Specifies how to read input (optional)
* `computation-layer`: optional. Specifies server settings for the Computation Layer
* `serving-layer`: optional. Specifies server settings for the Serving Layer

A simple, sound `oryx.conf` file:

```
model=${als-model}
model.instance-dir=/user/oryx/example
serving-layer.api.port=8091
computation-layer.api.port=8092
```

_The default ports are 80 and 8080 respectively, but, 80 requires `root` access to use, and 8080 is used by
Hadoop YARN daemon processes. So the config here specifies different ports._

## Running

```
java -Dconfig.file=oryx.conf -jar computation/target/oryx-computation-x.y.z.jar
sudo java -Dconfig.file=oryx.conf -jar serving/target/oryx-serving-x.y.z.jar
```

Note that the Serving Layer does not need to be run as `root` with `sudo` if it is configured to not use
the standard, privileged HTTP port, 80.

## Examples

Normally, input is ingested from the API or imported from external sources. For simplicity in the following examples,
it is simplest to manually supply input directly into the file system.

If using the normal distribution Computation Layer, and data is located under `/user/oryx/example/`:

```
hadoop fs -mkdir -p /user/oryx/example/00000/inbound
hadoop fs -copyFromLocal [data file] /user/oryx/example/00000/inbound/
```

For local use, using for example a temp directory `/tmp/oryx/example` for testing:

```
mkdir -p /tmp/oryx/example/00000/inbound
cp [data file] /tmp/oryx/example/00000/inbound/
```

### Collaborative filtering / Recommender Example

To demonstrate a recommender, data of the form `user,item` or `user,item,strength` is required. Identifiers in the
first two columns can be numeric or non-numeric, and represent any kind of entity. A numeric strength value is optional,
and can be for example simple rating information.

User and item IDs must be escaped using CSV conventions if necessary: double-quote a field containing comma,
and use two double-quotes to escape a double-quote within a quoted value.

For a demo, try downloading a [sample of the
Audioscrobbler data set](http://raw.github.com/wiki/cloudera/oryx/datasets/audioscrobbler-sample.csv.gz).

Example configuration, which will run computation locally:

```
model=${als-model}
model.instance-dir=/tmp/oryx/example
model.local-computation=true
model.local-data=true
model.features=25
model.lambda=0.065
```

The Computation Layer and Serving Layer consoles are available at `http://[host]:8080` and `http://[host]`
respectively.

Copy the data in manually, and start the servers, as above.
The Computation Layer should start computing immediately. The Serving Layer will load its output
shortly after it finishes.

Use the endpoint interface in the console, input your own preferences. Next to `/pref`,
type your name (e.g. `sean`) and type the name of a band in the data set, like `Bad Religion`.
Then `/recommend` to yourself. If you input this band, you should see
recommendations like this punk band, like `NOFX` or `Green Day`.

### Classification / Regression example

A classification or regression example takes as input CSV data, where one of the columns (the "target") is to
be predicted from values of the others. To use it, the input's columns must be named, their types given (numeric
or categorical) and the target indicated.

For demonstration purposes, obtain a copy of the `covtype` data set; download `covtype.data.gz` from the
[UCL repository](http://archive.ics.uci.edu/ml/machine-learning-databases/covtype/).

The following example configuration file works with this input:

```
model=${rdf-model}
model.instance-dir=/tmp/oryx/example
model.local-computation=true
model.local-data=true
inbound.numeric-columns=[0,1,2,3,4,5,6,7,8,9]
inbound.target-column=54
inbound.column-names=["Elevation", "Aspect", "Slope", "Horizontal_Distance_To_Hydrology",
  "Vertical_Distance_To_Hydrology", "Horizontal_Distance_To_Roadways", "Hillshade_9am", "Hillshade_Noon",
  "Hillshade_3pm", "Horizontal_Distance_To_Fire_Points", "Wilderness_Area1", "Wilderness_Area2",
  "Wilderness_Area3", "Wilderness_Area4", "Soil_Type1", "Soil_Type2", "Soil_Type3", "Soil_Type4",
  "Soil_Type5", "Soil_Type6", "Soil_Type7", "Soil_Type8", "Soil_Type9", "Soil_Type10", "Soil_Type11",
  "Soil_Type12", "Soil_Type13", "Soil_Type14", "Soil_Type15", "Soil_Type16", "Soil_Type17", "Soil_Type18",
  "Soil_Type19", "Soil_Type20", "Soil_Type21", "Soil_Type22", "Soil_Type23", "Soil_Type24", "Soil_Type25",
  "Soil_Type26", "Soil_Type27", "Soil_Type28", "Soil_Type29", "Soil_Type30", "Soil_Type31", "Soil_Type32",
  "Soil_Type33", "Soil_Type34", "Soil_Type35", "Soil_Type36", "Soil_Type37", "Soil_Type38", "Soil_Type39",
  "Soil_Type40", "Cover_Type"]
```

Copy the data in manually, and start the servers, as above.
The Computation Layer should start computing immediately. The Serving Layer will load its output
shortly after it finishes.

Use the endpoint interface in the console to test by calling `/classify` with a new line of CSV data that
does *not* contain a target value; here that means a missing final column. For example, send:

```
2500,51,3,258,0,510,221,232,148,6279,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,
 ```

and the result should be `2`, but will depend a bit on the way the trees build.

### Clustering example

Like classification and regression, the clustering algorithm takes as input a CSV file with named columns that
have their type (numeric or categorical) specified. An input row may also have an optional identifier column
that can be used for training and evaluation purposes, but is not used to assign points to clusters.

For demonstration purposes, we will use a dataset from the KDD99 Cup Challenge; obtain a copy of `kddcup.data_10_percent.gz` from
the [UCI KDD99 homepage](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html).

```
model=${kmeans-model}
model.instance-dir=/tmp/oryx/example
model.local-computation=true
model.local-data=true
model.sketch-points=50
model.k=[1, 5, 10]
model.replications=2
inbound.column-names=[duration, protocol_type, service, flag, src_bytes, dst_bytes,
  land, wrong_fragment, urgent, hot, num_failed_logins, logged_in, num_compromised,
  root_shell, su_attempted, num_root, num_file_creations, num_shells, num_access_files,
  num_outbound_cmds, is_host_login, is_guest_login, count, srv_count, serror_rate,
  srv_serror_rate, rerror_rate, srv_rerror_rate, same_srv_rate, diff_srv_rate,
  srv_diff_host_rate, dst_host_count, dst_host_srv_count, dst_host_same_srv_rate,
  dst_host_diff_srv_rate, dst_host_same_src_port_rate, dst_host_srv_diff_host_rate,
  dst_host_serror_rate, dst_host_srv_serror_rate, dst_host_rerror_rate,
  dst_host_srv_rerror_rate, category]
inbound.categorical-columns=[protocol_type, service, flag, logged_in, is_host_login,
  is_guest_login, category]
inbound.id-columns=[category]
```

Copy the data in manually, and start the servers, as above.
The Computation Layer should start computing immediately. The Serving Layer will load its output
shortly after it finishes.

Use the endpoint interface in the console to test by calling `/assign` or `/distanceToNearest` with
a new line of CSV data that does not contain a value for the identifier column. For example, send:

```
0,tcp,http,SF,259,14420,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,11,97,1.00,0.00,0.09,0.08,0.00,0.00,0.00,0.00
```

# [FAQ and Troubleshooting](https://github.com/cloudera/oryx/wiki/FAQ-and-Troubleshooting)

# [Performance and Quality](https://github.com/cloudera/oryx/wiki/Performance-and-Quality)

# [Javadoc](http://cloudera.github.io/oryx/apidocs/index.html)

# [Configuration Reference](https://github.com/cloudera/oryx/wiki/Configuration-Reference)

# [API Endpoint Reference](https://github.com/cloudera/oryx/wiki/API-Endpoint-Reference)
