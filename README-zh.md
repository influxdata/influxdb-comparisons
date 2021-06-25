# Influxdb-comparisons
这个报告包 `InfluxDB` 和其他数据库和时间序列解决方案的基准测试的代码。您可以在这里访问[技术的详细说明](https://influxdata.com/technical-papers/)。

当前支持的数据库：
- InfluxDB
- Elasticsearch
- MongoDB
- OpenTSDB

# 测试方法论
为了使我们的性能比较既现实又可靠，我们决定根据实际用例构建我们的基准测试套件。微基准测试对数据库工程师很有用，但是使用实际数据有助于我们更好地理解我们的软件在实际工作负载下的性能。

目前，基准测试工具主要关注 `DevOps` 用例。我们创建数据和查询，这些数据和查询模拟系统管理员在操作数百或数千台虚拟机时将看到的内容。我们创建并查询诸如 `CPU` 负载、`RAM` 使用、活动进程、休眠进程或停顿进程的数量以及所使用的磁盘之类的值。未来的基准测试将扩展到包括 `IOT` 和应用监视用例。

我们对批量加载性能和同步查询执行性能进行基准测试。基准测试套件是用 `Go` 编写的，并且通过消除与测试相关的计算开销（通过预先生成数据集和查询，并在可能的情况下使用特定于数据库的驱动程序），试图尽可能公平地对待每个数据库。

虽然数据是随机生成的，但我们的数据和查询完全是确定性的。通过向测试生成代码提供相同的 `PRNG`（伪随机数生成器）种子，每个数据库加载相同的数据，并使用相同的查询进行查询。
（注意：在编写和/或查询数据库时，使用多个工作线程确实会导致事件的非确定性排序。）

当使用基准测试套件时，有五个阶段：数据生成、数据加载、查询生成、查询执行和查询验证。

## 步骤一：生成数据
每个基准测试都从数据生成开始。

`DevOps` 数据生成器生成与服务器遥测相对应的时间序列点，类似于服务器机队定期向度量收集服务（如 Telegraf 或 collectd）发送的内容。我们的 `DevOps` 数据生成器对预定数量的主机运行模拟，并向标准输出发送序列化点。对于每个模拟机器，以 1 秒的间隔写入九个不同的度量。

`DevOps` 数据生成器的预期用途是创建不同的数据集，这些数据集模拟随着时间的增加而越来越大的服务器机群。随着主机计数或时间间隔的增加，点计数增加。这种方法允许我们检查数据库如何按照 `DevOps` 用户关心的维度在真实世界的工作负载上伸缩。

每个模拟主机用 `RAM` 大小和一组状态概率分布（高斯随机游走分布）初始化，对应于 `Telegraf` 报告的九个统计量。这里有用于 `CPU` 和内存的电报收集器：

```
https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/cpu.go 
https://github.com/influxdata/telegraf/blob/master/plugins/inputs/system/memory.go
```
例如，这里是使用数据生成器时10台主机的模拟CPU使用时间图：
(TODO screenshot of graph from Chronograf)

并且，这是来自同一模拟的模拟存储器的图表：
(TODO screenshot of graph from Chronograf)

注意，生成器在数据库之间共享其模拟逻辑。这不仅仅是为了代码质量；我们这样做是为了确保生成的数据在浮点容限内对每个数据库完全相同。

`DevOps` 数据集由以下参数完全指定：模拟（默认值 1）开始时间（默认2016年1月1日午夜，包括默认）结束时间（默认2016年1月2日午夜，不包括）伪随机生成器种子（默认使用当前时间）的主机数量

`DevOps` 生成器的”缩放变量“是要模拟的主机数量。默认情况下，数据是在一天的模拟期间生成的。每个模拟主机每1秒周期产生九个测量值，每个测量值一个：
- cpu
- diskio
- disk
- kernel
- mem
- net
- nginx
- postgresl
- redis
每个测量值保存不同的值。总共，所有九个测量值都存储 101 个字段值。

以下方程描述了 24 小时内生成多少点：

```
每天的秒数 = (24小时) * (60分钟) * (60秒) = 86,400 秒
点数 = 每天的秒数 * 101 = 8726400 
```
因此，对于一个主机，我们得到 `8726400` 点，对于 `1000` 个主机，我们得到 `8726400 * 1000=8726400000`点。

对于这些基准测试，我们生成一个数据集，我们称之为 `DevOps-100：100` 模拟主机，在不同的时间段（1-4天）。

生成的数据是以数据库特定的格式编写的，该格式直接等同于每个数据库的批量写入协议。这有助于使下面的基准测试（批量加载）尽可能简单。

对于 `InfluxDB`，大容量加载协议描述为：https://docs.influxdata.com/influxdb/v0.12/guides/writing_data/#writing-multiple-points

对于 `Elasticsearch`，批量加载协议描述为：https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

对于 `Cassandra`，本机协议版本4描述如下：https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec

对于 `MongoDB`，我们在 `mgo` 客户机上使用标准 `BSON`：http://labix.org/mgo

对于 `OpenTSDB`，我们使用以下描述的标准 `HTTP` 查询接口（不是批量输入工具）：http://opentsdb.net/docs/build/html/api_http/put.html


## 步骤二：装载数据
数据生成之后是数据加载。

数据加载程序从标准输入流读取数据；通常，这是来自数据生成器创建的文件。当读取数据时，加载器执行最少的反序列化和将写入排队到批处理中。随着批处理准备就绪，点将尽快加载到目标数据库中。

（每个数据库当前都有自己的批量加载程序。将来，我们希望将程序合并在一起，以便将特殊情况代码的数量最小化。）

### 配置
每个批量加载程序都接受一些影响性能的参数：

ElasticSearch: 用于并行进行批量加载写入的工作人员数量、要使用哪个索引模板（稍后将详细介绍）、是否在每次写入之后强制刷新索引，以及每个写入批次中包括多少项。

InfluxDB: 用于并行进行批量加载写入的工作人员数量，以及每个写入批中包括多少点。

其他数据库的加载程序采用类似的参数。

（对于校准，还有一个选项可以禁用对数据库的写入；该模式用于检查数据反序列化的速度。）

注意，如果在测试开始时目标数据库中已经有数据，则批量加载程序将不会开始写入数据。这有助于确保数据库是空的，就好像它是新安装的一样。它还防止用户破坏现有数据。

### Elasticsearch-specific 配置
`Elasticsearch` 和 `InfluxDB` 都已经为存储时间序列数据准备就绪。然而，在与 `Elasticsearch` 专家会面之后，我们决定对 `Elasticsearch` 进行一些合理的配置调整，以试图优化它的性能。

首先，`Elasticsearch` 守护进程的配置被更改为将 `ES_HEAP_SIZE` 环境变量设置为服务器机器可用 `RAM` 的一半。例如，在 `32GB` 机器上，`ES_HEAP_SIZE` 是`16GB`。这是管理 `Elasticsearch` 的标准实践。

其次，配置文件也被更改为将 `threadpool.bulk.queue_size` 参数增加到 100000。当我们在没有这种调整的情况下尝试批量加载时，服务器会以错误回复，指出它已经用完了用于接收批量写入的缓冲区。此配置更改是批量写入工作负载的标准实践。

第三，我们开发了两个 `Elasticsearch` 索引模板，每个模板代表我们认为人们使用 `Elasticsearch` 存储时间序列数据的一种方式：

第一个模板称为 “default”，它以一种能够快速查询的方式存储时间序列数据，同时还存储原始文档数据。这与 `Elasticsearch` 的默认行为最接近，并且是大多数用户的合理起点，尽管其在磁盘上的大小可能变大。

第二个模板称为“聚合”，它通过丢弃原始点数据来索引时间序列数据，从而节省磁盘空间。所有数据都以压缩形式存储在 `Lucene` 索引中，因此所有查询都是完全准确的。但是，由于弹性的实现细节，底层点数据不再能够独立寻址。对于只执行聚合查询的用户，这节省了相当多的磁盘空间（并提高了批量加载速度），没有任何缺点。

第四，在弹性搜索中的每个批量加载之后，我们触发对所有索引数据的强制压缩。这不包括在速度测量中，我们给弹性搜索“免费”。之所以选择这样做，是因为压缩在长期运行的Elasticsearch过程的生命周期中不断发生，因此这有助于我们获得代表生产环境中Elasticsearch的稳态操作的数字。

（注意，`Elasticsearch` 不会立即索引用批量端点编写的数据。为了使写入的数据立即可用于查询，用户可以将 `URL` 查询参数“刷新”设置为“true”。我们之所以没有这样做，是因为性能显著下降，并且大多数用户在执行批量加载时不需要这样做。`InfluxDB` 在每次批量写入之后执行 `fsync`，并使数据立即可用于查询。

### InfluxDB-specific 配置
我们对默认的 `InfluxDB` 安装所做的唯一更改是，像 `Elastic` 一样，在完成批量负载基准测试之后，导致完整的数据库压缩。这迫使所有最终的压缩同时发生，模拟数据存储的稳态操作。

### 指标
对于批量加载，我们关心两个数值结果：写入给定数据集所需的总时间，以及所有写入完成后数据库使用多少磁盘空间。

当完成后，批量加载程序打印出加载数据需要多长时间，以及平均写入速率是多少。

结合以下参数给出给定数据集的假设的“性能矩阵”：

```
Client parallelism: 1, 2, 4, 8, 16
Database: InfluxDB, Elasticsearch (with default template), Elasticsearch (with aggregation template)
```
这给出了一组可能的 `15` 个批量写基准。运行所有这些测试是过度的，但它是可能的，并且允许我们自信地确定写吞吐量和磁盘使用规模的大小。

## 步骤五： 查询验证
最后一步是通过采样两个数据库的查询结果来验证基准测试。

基准测试套件被设计成完全确定性的。但是，这不能防止数据或查询集中的可能语义错误。例如，如果一个数据库的查询计算不希望的结果，那么该查询可能是有效的，但也可能是错误的。

为了显示数据库之间的数据和查询的奇偶性，我们可以比较查询响应本身。

我们的查询基准测试工具有一个模式，用于美化打印它接收到的查询响应。通过在此模式下运行它，我们可以检查查询结果并比较每个数据库的结果。

例如，这里是对同一查询的响应的并排比较（最大值列表，在1分钟桶中）：

InfluxDB 查询响应：

```json
{
  "results": [
    {
      "series": [
        {
          "name": "cpu",
          "columns": [
            "time",
            "max"
          ],
          "values": [
            [
              "2016-01-01T18:29:00Z",
              90.92765387779365
            ],
            [
              "2016-01-01T18:30:00Z",
              89.58087379178397
            ],
            [
              "2016-01-01T18:31:00Z",
              88.39341429374308
            ],
            [
              "2016-01-01T18:32:00Z",
              84.27665178871197
            ],
            [
              "2016-01-01T18:33:00Z",
              84.95048030509422
            ],
            ...
```
Elasticsearch 查询响应：

```json
{
  "took": 133,
  "timed_out": false,
  "_shards": {
    "total": 5,
    "successful": 5,
    "failed": 0
  },
  "hits": {
    "total": 1728000,
    "max_score": 0.0,
    "hits": []
  },
  "aggregations": {
    "result": {
      "doc_count": 360,
      "result2": {
        "buckets": [
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451672940000,
            "doc_count": 4,
            "max_of_field": {
              "value": 90.92765387779365
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673000000,
            "doc_count": 6,
            "max_of_field": {
              "value": 89.58087379178397
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673060000,
            "doc_count": 6,
            "max_of_field": {
              "value": 88.39341429374308
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673120000,
            "doc_count": 6,
            "max_of_field": {
              "value": 84.27665178871197
            }
          },
          {
            "key_as_string": "2016-01-01-18",
            "key": 1451673180000,
            "doc_count": 6,
            "max_of_field": {
              "value": 84.95048030509422
            }
          },
          ...
```
通过检查，我们可以看到结果是（在浮点公差范围内）相同的。对于每个基准测试运行的查询的代表性选择，我们已经手工完成了此操作。
成功的查询验证意味着基准测试套件具有端到端的可再现性，并且在两个数据库之间是正确的。

# 开始
执行基准测试需要在系统上安装Go编译器和工具。请参阅 https://golang.org/doc/install 了解软件包下载和安装。一旦配置了 Go，您就可以继续安装和运行基准测试。

## 安装
运行 `benchmark` 需要为您想要测试的平台安装数据和查询生成器以及数据装载加载程序。例如，要为 `influxdb` 安装和运行负载测试，请执行：

```go
go get github.com/influxdata/influxdb-comparisons/cmd/bulk_data_gen github.com/influxdata/influxdb-comparisons/cmd/bulk_load_influx
```

这将从 `GitHub` 下载并安装最新的代码（包括依赖项）。检查 `cmd` 目录以获取要下载和安装的其他数据库实现。对于查询基准，请为您的平台安装查询生成器和基准执行器。例如，对于 `influxdb`：

```go
go get github.com/influxdata/influxdb-comparisons/cmd/bulk_query_gen github.com/influxdata/influxdb-comparisons/cmd/query_benchmarker_influxdb
```

## 帮助
对于任何模块，都可以使用 `-h` 参数运行可执行文件，它将打印命令行参数列表。例如：

```
-bash-4.1$ $GOPATH/bin/bulk_data_gen -h
Usage of /home/clarsen/go/bin/bulk_data_gen:
  -debug int
    	Debug printing (choices: 0, 1, 2) (default 0).
  -format string
    	Format to emit. (choices: influx-bulk, es-bulk, cassandra, mongo, opentsdb) (default "influx-bulk")
  -interleaved-generation-group-id uint
    	Group (0-indexed) to perform round-robin serialization within. Use this to scale up data generation to multiple processes.
  -interleaved-generation-groups uint
    	The number of round-robin serialization groups. Use this to scale up data generation to multiple processes. (default 1)
  -scale-var int
    	Scaling variable specific to the use case. (default 1)
  -seed int
    	PRNG seed (default, or 0, uses the current timestamp).
  -timestamp-end string
    	Ending timestamp (RFC3339). (default "2016-01-01T06:00:00Z")
  -timestamp-start string
    	Beginning timestamp (RFC3339). (default "2016-01-01T00:00:00Z")
  -use-case string
    	Use case to model. (choices: devops, iot) (default "devops")
```

## 装载数据
要生成数据并将其写入数据库，请使用可选的命令行参数执行批量数据生成器，并将输出通过管道传输到批量加载程序。例如，在 `influxdb` 实例中加载数据，运行：

```go
$GOPATH/bin/bulk_data_gen | $GOPATH/bin/bulk_load_influx -urls http://localhost:8086
```

这将自动创建一个数据库实例并加载大约 `19440` 个数据点。对于其他数据，请设置开始和结束时间。还要注意，默认的生成数据格式是 `influx bulk`。如果要测试另一个数据库，请在适当的加载程序中使用 `-format` 参数。例如，对于 `OpenTSDB`：

```go
$GOPATH/bin/bulk_data_gen -format opentsdb | $GOPATH/bin/bulk_load_opentsdb -urls http://localhost:4242
```
成功运行将生成和存储的项目数以及每秒的总时间、平均速率。

```go
-bash-4.1$ $GOPATH/bin/bulk_data_gen | $GOPATH/bin/bulk_load_influx  -urls http://druidzoo-1.yms.gq1.yahoo.com:8086
using random seed 329234002
daemon URLs: [http://druidzoo-1.yms.gq1.yahoo.com:8086]
[worker 0] backoffs took a total of 0.000000sec of runtime
loaded 19440 items in 0.751433sec with 1 workers (mean rate 25870.568346/sec, 8.60MB/sec from stdin)
```

## 查询数据测试

查询数据库类似于加载数据。执行批量查询生成器，并将其输出通过管道传输到测试数据库的基准工具。每次运行都需要 `-query-type` 参数来确定要执行的查询类型。这些是为了模拟实际的查询，例如从多个主机中搜索单个主机上的数据，或者通过不同的标记进行分组。要找出可用的查询类型，执行 `$gopath/bin/bulk-query-gen-h` 并在输出的底部查找用例矩阵。运行命令示例如下：

```go
$GOPATH/bin/bulk_query_gen -query-type "1-host-1-hr" | $GOPATH/bin/query_benchmarker_influxdb -urls http://druidzoo-1.yms.gq1.yahoo.com:8086
```

成功的运行将执行多个查询，并定期将状态信息打印到标准输出。

```bash
-bash-4.1$ $GOPATH/bin/bulk_query_gen -query-type "1-host-1-hr" | $GOPATH/bin/query_benchmarker_influxdb -urls http://druidzoo-1.yms.gq1.yahoo.com:8086
using random seed 684941023
after 100 queries with 1 workers:
Influx max cpu, rand    1 hosts, rand 1h0m0s by 1m : min:     1.50ms ( 668.55/sec), mean:     1.98ms ( 506.32/sec), max:    3.10ms (322.34/sec), count:      100, sum:   0.2sec
all queries                                        : min:     1.50ms ( 668.55/sec), mean:     1.98ms ( 506.32/sec), max:    3.10ms (322.34/sec), count:      100, sum:   0.2sec

...

run complete after 1000 queries with 1 workers:
Influx max cpu, rand    1 hosts, rand 1h0m0s by 1m : min:     1.45ms ( 689.62/sec), mean:     2.07ms ( 482.67/sec), max:   12.21ms ( 81.92/sec), count:     1000, sum:   2.1sec
all queries                                        : min:     1.45ms ( 689.62/sec), mean:     2.07ms ( 482.67/sec), max:   12.21ms ( 81.92/sec), count:     1000, sum:   2.1sec
wall clock time: 2.084896sec
```
