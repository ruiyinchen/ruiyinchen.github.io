使用 Spark 结构化流探索状态流
BLANK SPACE 2018-12-26   Stateful Streaming
在之前的文章中，我们探讨了如何使用带DStream抽象的Sparks Streaming API进行处理状态流。今天，我想和你一起探索Spark 2.2，它在Structured Streaming API下支持状态流。在这篇文章中，我们将看到API如何成熟和发展，看看两种方法（流与结构化流）之间的差异，并了解对API进行了哪些更改。我们将通过上一篇文章中的示例，并使其适应新的API来实现。

回顾Spark Streaming状态管理
如果你需要在Spark中使用状态流，你必须在两个抽象之间进行选择（直到Spark 2.2）。updateStateByKey 和 mapWithState后者或多或少是前者的改进（API和性能方面）版本（具有一些不同的语义）。为了利用微批次之间的状态，你提供了一个StateSpec函数，mapWithState对于到达当前微批次的每个键值对，将调用该函数。对于 mapWithState，主要优点是：

状态的初始RDD - 可以加载具有先前保存状态的RDD
超时 - 超时管理由Spark处理。你可以为所有键值对设置单个超时。
部分更新 - 仅迭代当前微批中“触摸”的键以进行更新
返回类型 - 你可以选择任何返回类型。
但事情并不总是完美的……

mapWithState 的痛点
mapWithState比以前的 updateStateByKey API 有了很大的改进。但是去年在我使用它的过程中，体验过一些缺点：

检查点
为了确保Spark可以从失败的任务中恢复，它必须将数据 checkpoint 到分布式文件系统中，它可以在失败时从中恢复。使用 mapWithState 时，每个 executor 在内存中保存已累积的所有状态的 HashMap。在每个检查点，Spark 每次都会序列化整个状态。如果你在内存中持有很多状态，这可能会导致严重的处理延迟。例如，以下设置：

批处理间隔：4秒
检查点间隔：40秒（4秒批次x 10 常量 spark 系数）
每秒80,000条消息
消息大小：500B - 2KB
5 m4.2xlarge机器（8个vCPU，32GB RAM）
每台机器2个 executor
executor 存储大小~7GB（每个）
将数据 checkpoint 到 HDFS
我经历了长达4小时的累积延迟，因为高负荷下的每个检查点在整个状态下花费30秒-1分钟，我们每4秒产生一次批次。我也看到人们在StackOverflow上对此感到困惑，因为实际上并不明白为什么有些批次比其他批次花费的时间要长得多。

如果你计划使用状态流来实现高吞吐量，则必须将此视为一个严重的警告。这个问题非常严重，以至于它让我找到了在Spark中使用内存状态的替代方法。但我们很快就会看到事情看起来很光明;）

在版本更新之间保存状态
软件是一个不断发展的过程，我们总是在改进，增强和实现新的功能要求。因此，我们需要能够从一个版本升级到另一个版本，最好不要影响现有数据。在内存数据中，这变得非常棘手。我们如何保持现状？我们如何确保从中断的地方继续？

开箱即用，mapWithState不支持改进我们的数据结构。如果你已经修改了存储状态的数据结构，则必须删除所有以前的检查点数据，因为serialVersionUID对象版本之间会有所不同。此外，由于我们要从检查点恢复连接，因此对定义在 StreamingContext 上的执行图的任何更改都不会生效。

mapWithState确实提供了一种通过查看数据当前状态快照的方法MapWithStateDStream.stateSnapshot()。这使我们能够将状态存储在外部存储库中，并能够使用 StateSpec.initialRDD 让其从中恢复。但是，在外部存储数据会因检查点延迟而增加本就已经很大的延迟。

每个状态对象单独超时
mapWithState允许我们通过设置所有状态的默认超时StateSpec.timeout。但是，有时可能希望每个状态对象具有单独的状态超时。例如，假设我们要求用户会话不超过30分钟。然后出现了一个新客户，希望看到用户会话每10分钟结束一次，我们该怎么办？好吧，我们无法处理这个问题，我们必须实现我们自己的超时机制。更大的问题是，mapWithState只接触我们在当前批次中有数据的键值对，它不会触及所有键。这意味着我们必须将角色返回到updateStateByKey默认情况下迭代整个状态，这可能对性能有害（当然，取决于使用情况）。

单个 executor 故障导致数据丢失
executor 是java进程，对于任何进程，它们都可能失败。我在生产中遇到了堆损坏导致单个 executor 死亡。这个问题是，一旦Worker进程创建了新的 executor，它就不会从检查点恢复状态。如果你看一下CheckpointSuite测试，你会发现所有这些测试都涉及StreamingContext恢复，但是没有任何一个 executor 失败。

好的，缺点，很好。你正在谈论的这个新API在哪里？
不要着急，我们才刚刚开始…… :)

介绍结构化流
结构化流是Spark 的新的闪亮工具，用于推出流。

来自结构化流文档 - 概述：

结构化流是一种基于Spark SQL引擎的可扩展且容错的流处理引擎。你可以像表达静态数据的批处理计算一样表达流式计算。Spark SQL引擎将负责逐步和连续地运行它，并在流数据继续到达时更新最终结果。

Spark 的作者意识到，关于分布式流应用程序的推理存在许多隐藏的问题，除了维护业务领域逻辑之外，他/她可能会或可能没有意识到他/她必须处理。他们不是要处理所有这些问题，而是希望我们以与使用静态SQL表格相同的方式推断我们的流处理，同时生成查询，同时在新数据进入流中时对其进行运行。把它想象成一个无限的数据表。

有关结构化流和Dataset[T]抽象的更深入解释，请参阅DataBricks的这篇精彩文章。别担心，我等你..

欢迎回来。让我们继续看看结构化流中的新状态流抽象是什么样的

通过一个例子学习
我将在这里使用的示例与我之前关于状态流的帖子中使用的示例相同。回顾一下（对于那些不熟悉上一篇文章的人），该示例讨论了一组传入的用户事件，我们希望在它们从流中进入时进行聚合。我们的活动在 UserEvent 类上建模：

1
case class UserEvent(id: Int, data: String, isLast: Boolean)
我们通过他的 id 唯一地识别用户。此 id 将用于将传入数据组合在一起，以便我们将所有用户事件都发送到处理状态的同一 executor 进程。用户事件还具有一个 data 字段以生成一些String内容, 还有一个额外的 Boolean字段以指示这是否是当前会话的最后一条消息。

我们在流中汇总用户事件的方式是使用UserSession类：

1
case class UserSession(userEvents: Seq[UserEvent])
它将特定用户的所有事件保存在一起。

介绍mapGroupsWithState：
如果你认为“嗯，这个名字听起来很熟悉”，那你说对了，它几乎与我们在Spark Streaming中的 mapWithState 抽象相同，只是对面向用户的API进行了微小的改动。但首先，我们来谈谈两者之间的一些关键差异。

mapWithState和mapGroupsWithState（通用 Spark 结构化流 VS 流 ）之间的差异
在应用程序更新之间保持状态 - 最大的一个警告mapWithState是，除非你推出自己的状态簿记，否则你将被迫在升级之间删除内存数据。不仅如此，如果Spark DAG中的任何内容发生变化，你也必须删除该数据。从我的实验来看mapGroupsWithState，似乎使用Kryo编码器结合正确版本化数据结构（即使用新添加状态的默认值），允许你在应用程序升级之间保持数据并且还更改定义转换的Spark DAG并且仍然保持国家。对于任何使用这个人来说，这都是重mapWithState。我之所以谨慎说这是因为我没有看到任何来自结构化流开发者的官方文档或声明来支持这种说法。
微批处理执行 - Spark Streaming 需要固定的批处理间隔，以便使用来自源的数据生成和执行微批处理。即使没有新数据到达，微批次仍将执行，这将导致整个图执行。结构化流是不同的，它有一个专用线程，不断检查源，看是否有新数据到达。如果没有可用的新数据，则不会执行查询。那是什么意思？例如，这意味着如果你将超时间隔设置为X秒但新数据尚未进入流中，则任何状态都不会超时，因为它不会运行查询。
状态的内部数据存储 - mapWithState基于OpenHashMapBasedStateMap[K, V]存储内存状态的实现。mapGroupsWithState用途java.util.ConcurrentHashMap[K, V]。另外，后者使用一个名为UnsafeRowkey和value 的底层结构，而不是普通的JVM对象。这些不安全的行是由编码器为键和值生成的数据字节周围的包装器，并且当需要将键值对传递给我们的状态函数时，在不安全表示与我们的JVM对象结构之间应用按需转换。
状态版本控制 - 内存中不再有每个执行程序的单个状态存储。新实现使用HDFSBackedStateStore每个版本的状态，这意味着它只需要将最新版本的状态保留在内存中，同时让旧版本驻留在后备状态存储中，根据需要按需加载它们。
超时 - mapWithState为所有状态对象设置一个超时。mapGroupsWithState启用每个组的状态超时，这意味着你可以为超时创建更复杂的配置。此外，还可以根据事件时间和水印进行超时。
检查点 - mapWithState检查点每隔固定间隔发生一次。如果我们的批处理时间是4秒，则内存中的整个状态的检查点将每40秒发生一次。不仅如此，检查点是一个阻塞操作，这意味着在完成之前我们无法处理下一批的传入事件。对于更新的密钥，mapGroupsWithState检查点是递增地完成的，并且这被FileSystem用作状态存储的底层的实现抽象掉了。这意味着检查点开销应该显着减少。
偏移处理（对于可重放的源，例如Kafka） - 将DStreamAPI与可重放的源（如Kafka）一起使用，需要我们在持久存储（例如ZooKeeper，S3或HDFS）中推断偏移存储。从特定偏移重放某个源意味着从持久存储中读取数据并KafkaUtil.createDirectStream在初始化流时将其传递给它。结构化流式传输在重新运行应用程序时代表我们存储和检索偏移，这意味着我们不再需要在外部存储它们。
好吧，通过比较，让我们开展业务。

分析API
让我们看一下 mapGroupsWithState 的方法签名：

1
2
3
def mapGroupsWithState[S: Encoder, U: Encoder](
      timeoutConf: GroupStateTimeout)(
      func: (K, Iterator[V], GroupState[S]) => U)
让我们分解每个参数，看看我们能用它做些什么。第一个参数包含一个timeoutConf, 它负责我们想选择哪个超时配置。我们有两种选择：

基于处理时间（GroupStateTimeout.ProcessingTimeTimeout） - 基于恒定间隔的超时（类似于在Spark Streaming中在 StateSpec 上调用的 timeout函数）
基于事件时间（GroupStateTimeout.EventTimeTimeout） - 基于用户定义的事件时间和水印的超时（有关使用水印处理迟到数据的更多信息阅读此内容）。
在第二个参数列表中，我们有状态函数。让我们来看看每个参数及其含义：

1
func: (K, Iterator[V], GroupState[S] => U)
有三种参数类型，K和Iterator[V]，以及GroupState[S], 还有 U 类型的返回类型。让我们将这些参数映射到我们的示例并填写类型。

正如我们所见，我们有一个 UserEvent 类型的传入消息流。该类有一个 Int 类型的字段, 叫 id，我们将其用作将用户事件组合在一起的键。这意味着我们用 Int 替换 K：

1
(Int, Iterator[V], GroupState[S]) => U
接下来是Iterator[V]。V是我们将聚合的值的类型。我们将收到一个 UserEvent 流，这意味着我们需要用 UserEvent 替换V：

1
(Int, Iterator[UserEvent], GroupState[S]) => U
很好！哪个类描述了我们的状态？如果你向上滚动一下，你会发现我们已经定义了一个叫做UserSession 的类, 它勾勒出用户的整个会话，这就是我们用作状态类型的东西！让我们来用UserSession 替换 S

1
(Int, Iterator[UserEvent], GroupState[UserSession]) => U
太棒了，我们设法填写了参数的类型。返回类型，U是我们剩下的。我们只想在用户会话完成时才返回一个 UserSession, 要么通过用户会话超时要么接收到设置为 ture 的 isLast 标志。我们会把返回类型设置为 Option[UserSession], 如果我们完成了会话，它会被填充。这意味着用 Option[UserSession] 替换 U：

1
(Int, Iterator[UserEvent], GroupState[UserSession]) => Option[UserSession]
欢呼！

GroupStateAPI
对于熟悉mapWithStateAPI的人，GroupState应该对State类非常熟悉。让我们看看API是什么样的：

def exists: Boolean - 状态是否存在。
def get: S - 获取状态值（如果存在），或抛出 NoSuchElementException。
def getOption: Option[S] - 获取状态值作为 Scala Option[T]。
def update(newState: S): Unit - 更新状态值。请注意，null不是有效值，并抛出IllegalArgumentException。
def remove(): Unit - 删除状态。
def hasTimedOut: Boolean - 是否因为键已超时而调用了该函数。只有在[map/flatmap]GroupsWithStates 中启用超时时，才能返回 true
def setTimeoutDuration(durationMs: Long): Unit - 设置此键的超时持续时间（以毫秒为单位）。注意必须在 [map/flatmap]GroupsWithStates 中启用 ProcessingTimeTimeout，否则抛出UnsupportedOperationException
状态API的最新成员是setTimeoutDuration方法。我只包含一个重载，但还有另外3个接收各种输入参数类型，如String和java.util.Timestamp。请注意，由于每个组状态都可以拥有自己的超时，因此我们必须在 mapGroupsWithState 内部为每个组状态设置超时。这意味着每次调用我们的方法时，我们都必须使用 setTimeoutDuration 再次设置超时，正如我们在该方法时所看到的那样。

此外，调用setTimeoutDuration 有几个限制。如果我们没有在 mapGroupsWithState 中设置timeoutConf参数，当我们调用这个方法时它会抛出一个UnsupportOperationException，所以请确保你已经配置了超时。

我在这里总结了API文档，但如果你想要完整的详细信息，请参阅Scala文档。

创建自定义编码器
对于读者之间的洞察力，你可能已经注意到以下对 mapGroupsWithState 的类型参数的约束

1
def mapGroupsWithState[S: Encoder, U: Encoder]
这个 Encoder 类要求什么, 通过在元素 S 和 U 上context bound？类文档说：

用于将 JVM 类型的对象转换为 Spark SQL 内部表示形式的 T 类型或从 Spark SQL 内部表示形式转换为 T 类型

Spark SQL位于一个名为Catalyst Optimizer的优化器之上，该优化器是作为Project Tungsten的一部分创建的。Spark SQL（和Structured Streaming）负责处理原始字节而不是JVM对象，以优化空间和高效的数据访问。为此，我们必须告诉Spark如何将我们的JVM对象结构转换为二进制文件，这正是这些编码器所做的。

没有关于编码器的长度，这里是特征的方法签名：

1
2
3
4
5
6
7
8
9
10
trait Encoder[T] extends Serializable {

  /** Returns the schema of encoding this type of object as a Row. */
  def schema: StructType

  /**
   * A ClassTag that can be used to construct an Array to contain a collection of `T`.
   */
  def clsTag: ClassTag[T]
}
任何编码器都必须提供两个东西，一个是通过 StructType 描述类的模式，它是一个递归数据结构，用于布置我们描述的对象中每个字段的模式，以及ClassTag[T]用于转换T 类型的集合。

如果我们仔细查看 mapGroupsWithState 的签名，我们会看到我们需要提供两个编码器，一个用于由类型 S 表示的状态类，另一个用于由类型 U 表示的返回类型。在我们的例子中，这意味着以 Encoder[UserSession] 形式为 UserSession 提供隐式证据。但是我们如何生成这样的编码器呢？Spark通过SQLImplicits对象打包了基元的编码器，如果我们使用case类，我们必须有一个隐式的作用域。最简单的方法是使用 Encoders.kryo[T] 创建自定义编码器。我们这样使用它：

1
2
3
4
object StatefulStructuredSessionization {
  implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
  implicit val userSessionEncoder: Encoder[UserSession] = Encoders.kryo[UserSession]
}
有关自定义编码器的更多信息，请参阅此StackOverflow答案。

实现我们的状态方法
在弄清楚我们的状态方法的签名是什么之后，让我们继续实现它：

1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
def updateSessionEvents(
  id: Int,
  userEvents: Iterator[UserEvent],
  state: GroupState[UserSession]): Option[UserSession] = {
if (state.hasTimedOut) {
  // We've timed out, lets extract the state and send it down the stream
  state.remove()
  state.getOption
} else {
  /*
    New data has come in for the given user id. We'll look up the current state
    to see if we already have something stored. If not, we'll just take the current user events
    and update the state, otherwise will concatenate the user events we already have with the
    new incoming events.
  */
  val currentState = state.getOption
  val updatedUserSession = currentState.fold(UserSession(userEvents.toSeq))(currentUserSession => UserSession(currentUserSession.userEvents ++ userEvents.toSeq))
      
  if (updatedUserSession.userEvents.exists(_.isLast)) {
    /*
    If we've received a flag indicating this should be the last event batch, let's close
    the state and send the user session downstream. 
    */
    state.remove()
    updatedUserSession
  } else {  
    state.update(updatedUserSession)   
    state.setTimeoutDuration("1 minute")
    None
   }
  }
}
我们的updateUserEvents方法必须处理几个流。我们检查我们的方法是否因为状态超时被调用，如果超时，则state.hasTimedOut方法将被设置为 true并且我们的userEvents迭代器将为空。我们所要做的就是删除状态并把我们的 Option[UserSession] 发送到下游的流。如果它没有超时，则意味着已经调用了该方法，因为新值已经到达。

我们将当前状态提取到currenState值并通过 Option.fold处理以下两种情况：

状态为空 - 这意味着这是给定键的第一批值，我们所要做的就是获取我们收到的用户事件并将它们提升到UserSession类中。
状态有一个值 - 我们从UserSession对象中提取现有的用户事件（这是 fold 方法中参数列表的第二个参数）并将它们附加到我们刚收到的新值上。
UserEvent类包含一个名为 isLast 的字段，我们检查这个字段，看看它是不是会话中最后一个进入的事件。在聚合值之后，我们扫描用户事件序列以查看我们是否已收到该标志。如果我们确实收到了这个标志，那么我们从状态中删除会话并返回该会话，否则我们更新状态并设置超时持续时间并返回None以指示会话尚未完成。

将它们整合在一起
我们已经看到了如何定义状态方法，这意味着我们已经准备好创建执行图。在这个例子中，我将消费 socket 中的 JSON 结构的数据, 该 JSON 结构我们的 UserEvent 匹配。

我提前道歉，我没在 gist 中包括所有代码和导入。在帖子的底部，你将找到一个指向 GitHub 上完整工作仓库的链接，其中包含你可以尝试的所有代码。

首先，我们用带有 master URI 的详情和应用程序名创建了一个 SparkSession 实例：

1
2
3
4
val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Stateful Structured Streaming")
      .getOrCreate()
SparkSession是我们与流图交互的网关，就像StreamingContext以前做的那样。在我们定义会话之后，我们表达了我们的源的格式用于消费数据，在这种情况下它是一个 Socket：

1
2
3
4
5
6
7
8
import spark.implicits._

val userEventsStream: Dataset[String] = spark.readStream
    .format("socket")
    .option("host", host)
    .option("port", port)
    .load()
    .as[String]
导入spark.implicits._是为基元定义的编码器（我们在这里使用它String）。host和port变量来自于命令行参数。一旦我们调用该load()方法，我们就会返回DataFrame。可以将其视为包含行和列的数据的通用表示。为了将a转换DataFrame为a，DataSet[String]我们使用as[T]告诉我们想要使用的Spark 的方法获取一个类型化的数据集。

在我们掌握了数据集之后，我们可以map通过它将我们的JSON反序列化为a UserEvent并将我们的状态方法应用于它：

1
2
3
4
5
6
val finishedUserSessionsStream: Dataset[UserSession] =
  userEventsStream
    .map(deserializeUserEvent)
    .groupByKey(_.id)
    .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateSessionEvents)
    .flatMap(userSession => userSession)
在映射数据集并反序列化所有事件之后，我们使用groupByKey它们的id对用户事件进行分组以返回a KeyValueGroupedDataset[K, V]。分组是在键值mapGroupsWithState数据集类型上定义的暴露的关键（没有双关语）。然后我们调用mapGroupsWithState并传递GroupStateTimeout.ProcessingTimeTimeout第一个参数来向Spark指示我们想要超时的状态，并传入我们updateSessionEvents事先定义的方法。在我们完成应用有状态转换之后，我们将完成的会话输出到图的其余部分，但是None如果会话未完成，我们也会输出。这意味着我们必须确保只保留Option[T]包含a的流动Some[UserSession]，这就是原因flatMap。

我们留下来定义流的输出。在本例中，我选择了“console”格式，它只是将值打印到控制台，但你可以使用任何现有的输出接收器。此外，我们必须指定我们选择哪个类型的输出模式（对于 mapGroupsWithState 我们只能用 OutputMode.Update）和检查点目录位置：

1
2
3
4
5
6
finishedUserSessionsStream.writeStream
  .outputMode(OutputMode.Update())
  .format("console")
  .option("checkpointLocation", "hdfs:///path/to/checkpoint/location")
  .start()
  .awaitTermination()
瞧，看着数据开始涌入流中！

包起来
结构化流带来了关于流的推理的新心智模型。对于 mapGroupsWithState 我最希望看到的功能是，由于处理状态聚合的方式，我们不再受到检查点的惩罚。此外，能够在版本升级之间保存状态并获得自动偏移管理也非常有吸引力。时间肯定会告诉我们这是否是我们一直在等待的状态管理框架，它看起来确实很有希望。

有更多内部实现细节很有趣，我鼓励你在开始使用 Structured Streaming 时在评论中进行探索和分享。

你可以在我的GitHub 状态流式存储库中找到完整的工作示例。
