1. 

题目有些长：
一个类有三个方法：
1. createGrant(grant_id, amount, ts, exp_ts)
    -exp_ts 是到期时间，到了那个时间，这个grant就会清零
2. subtract(amount)
    -subtract的时候，按到期时间升序来consume
    -如果不足够balance去 subtract，getBalance 时 throw exception
3. getBalance(ts)
    -得出ts那个时刻的balance 或 throw exception
---------------------------------------
test case1:
            gpc.subtract(1, 30);
           assertThrows(InsufficientCreditException.class, () -> gpc.getBalance(30));
            assertThrows(InsufficientCreditException.class, () -> gpc.getBalance(40));
            gpc.createGrant("a", 1, 10, 100);
            Assertions.assertEquals(1, gpc.getBalance(10));
            Assertions.assertEquals(1, gpc.getBalance(20));
            Assertions.assertEquals(0, gpc.getBalance(30));
------------------------------------
testcase2:
            gpc.createGrant("a", 3, 10, 60);
            Assertions.assertEquals(3, gpc.getBalance(10));
            gpc.createGrant("b", 2, 20, 40);
            gpc.subtract(1, 30);
            gpc.subtract(3, 50);
            Assertions.assertEquals(3, gpc.getBalance(10));
            Assertions.assertEquals(5, gpc.getBalance(20));
            Assertions.assertEquals(4, gpc.getBalance(30));
            Assertions.assertEquals(3, gpc.getBalance(40));
            Assertions.assertEquals(0, gpc.getBalance(50));

====================================================================================



// https://www.1point3acres.com/bbs/thread-1098314-1-1.html
// gpu credit
Scan line ????


GPU Credit
====================================================================================


Implement a GPU credit calculator.


```python
class GPUCredit:


def addCredit(creditID: str, amount: int, timestamp: int, expiration: int) -> None：
 # A credit is an offering of GPU balance that expires after some expiration-time. The credit can be used only during [timestamp, timestamp + expiration]. **Check with your interviewer whether this period is inclusive '[]' or exclusive '()'. Examples given were inclusive.** A credit can be repeatedly used until expiration.

def getBalance(timestamp: int) -> int | None: # return the balance remaining on the account at the timestamp, return None if there are no credit left. Note, balance cannot be negative. See edge case below.

def useCredit(timestamp: int, amount: int) -> None
```


Example 1:


```python
gpuCredit = GPUCredit()
gpuCredit.addCredit('microsoft', 10, 10, 30)
gpuCredit.getBalance(0) # returns None
gpuCredit.getBalance(10) # returns 10
gpuCredit.getBalance(40) # returns 10
gpuCredit.getBalance(41) # returns None
```


Example 2:


```python
gpuCredit = GPUCredit()
gpuCredit.addCredit('amazon', 40, 10, 50)
gpuCredit.useCredit(30, 30)
gpuCredit.getBalance(40) # returns 10
gpuCredit.addCredit('google', 20, 60, 10)
gpuCredit.getBalance(60) # returns 30
gpuCredit.getBalance(61) # returns 20
gpuCredit.getBalance(70) # returns 20
gpuCredit.getBalance(71) # returns None
```


Edge Case:


```python
gpuCredit = GPUCredit()
gpuCredit.addCoupon('openai', 10, 10, 30)
gpuCredit.useCoupon(10, 100000000)
gpuCredit.getBalance(10) # returns None
gpuCredit.useCoupon('openai', 10, 20, 10)
gpuCredit.getBalance(20) # returns 10
```


Now, what if all the timestamps is not mono increasing?

楼主今天重新想了下这道题
觉得这个思路code最简单

两个list
一个list存cost
另一个list存credit

在get_balance（timestamp）里
1) 用credit list建两个list，一个list存timestamp前过期的credit, expired_credit_list另一个list存timestamp还valid的credit, valid_credit_list
2）用cost list建一个list: valid_usage_list，就是把在timestamp前的usage存起来就好

然后跑四个while
第一个while iterate所有的valid_usage_list里的cost, 用expired credit list把所有可以抵扣的usage抵扣掉，然后更新valid_usage_list，该消消，该抵扣抵扣

第二个while 再iterate所有剩下的valid_usage_list，这次用valid_credit_list抵扣，该消消该抵扣抵扣

上面两个iterate完之后，要不valid_usage_list是空的要不valid_credit_list是空的

建一个balance = 0


第三个while 就是iterate 剩下的valid_usage_list（如果不为空），全部amount扣进balance

第四个while就是iterate剩下的valid_credit_list（如果不为空），全部amount加进balance

最后看balance 是否小于等于0 以及看原来的credit list是否有credit在timestamp时候active来判断输出None，0，还是一个正数

——
自己简单写写后大概50分钟写完了也过了test case

诶只能说刷leetcode对这道题无益，这道题越暴力解，代码越简单

太可惜了，楼主无力吐槽
====================================================================================


2.
https://www.1point3acres.com/bbs/thread-1115238-3-1.html

之前认真准备了地里的题，自己基本都写了一遍，最后遇到一道地里信息最少的，跟之前猜的不一样。一同发上来之前整理的信息，也算回馈一下地里，求大米



====================================================================================
track balance，就三个func：create_grant（id，amount，timestamp，expiretime）、subtract（amount，timestamp ）、get_balance（timestamp）。每次grand有id，amount，timestamp，expiration_timestamp。完全用timestamp控制get_balance，取钱的时候优先从最老的grand取，但是不需要返回，只有get_balance的时候返回balance，可能会先subtract后create，不会get到负数


====================================================================================
sql：Memory db, insert, query, where filter, order by。select(table_name, where=None, order_by=None) ， 多个where的情况下只支持and。Query with where condition。Query with where condition on multiple columns。Query with where condition and order by one column。Query with where condition and order by multiple columns

what if deletion

====================================================================================
Excel sheets, getCell(), setCell(), handle cycles in sheets. Expected to write tests. First-part is OK to be implemented with sub-optimal getCell() implementation where the value is computed on the fly. Second part, the setCell() is supposed to update the values of impacted cells so that getCell is O(1)


====================================================================================
Resumable iterator：
前两问要求implement a resumable iterator with getState()/setState() for list，你可以用index来实现。但是第三问要求实现一个MultipleResumableFileIterator with existing ResumableFileIterator to iterator multiple json files，同时还要handle empty file case。建议大家都自己写一下，idea应该是combine Leetcode 251 with resumable iterator。
Resumable iterator：getState会需要一个专门的state class，返回的state作为setState的参数。比如是list iterator [1, 2, 3, 4] next -> 1 next -> 2 getState -> saved 2 next -> 3 setState(2) -> back to 2


====================================================================================
第一问，写个interface，写个test，这一问不用run test （1）实现接口
  getState会需要一个专门的state class，返回的state作为setState的参数。
  比如是list iterator [1, 2, 3, 4]
  next -> 1  next -> 2  getState -> saved 2  next -> 3  setState(2) -> back to 2
第二问，具体实现一个resumable list iterator， 写个简单的可恢复迭代器 ResumableIterator
第三问，写一个可恢复的list of 迭代器。ResumableMultiFileIterator （3）给定file iterator的实现，要求完成multiple file iterator， 同时还要handle empty file case。idea应该是combine Leetcode 251 with resumable iterator。
第四问，上corotine，把每个迭代器改成async ResumableFileIterator （4）实现efficient multiple file iterator

第三问，2D resumable list iterator，给你一个已经写好的file-iterator在这个上面构建一个2d iterator。整体不难，但是有很多细节需要注意，特别是corner case，next() 需要用到recursive
第四问，3d iterator

====================================================================================

Implement a time-based versioned kv store. you can use real timestamp as input. think about how to write tests, how to mock timestamp, how to make sure timestamp monotonically increase, how to lock if multithreading, compare the performance of various lock implementation.

====================================================================================
写一个KVStore class，要求能set，get value，还要能存进存进file system 再resume出来。key，value是string，可能包含任何字符。这一问应该主要考察的是serialize/deserialize。

题目要求不可以用python自带的serialization library （包括这里的json），要自己写serialization/deserialiation，并且key，value里面可能有任何字符，包
括换行符之类的。

====================================================================================

题目是给一个tree，里面的每一个node都是一个machine，只有parent和child之间可以通讯。通讯是靠sendAsyncMessage()和receiveMessage()， 那个sendmessage不需要自己实现，属于提供的一个interface，可以直接用。现在需要你设计一个方法，去统计一共有多少machine。这是第一问，相对比较容易一些，理解他想让你干嘛之后，就实现这个receiveMessage的method就好，基本的逻辑就是，判断Message，如果是count，就发同样消息给自己的child。如果是response的话，就把count number记录下来，等把所有child都记录下来之后，做一个sum，然后返回给自己的parent。里面有一些case handle好就没问题 （比如当是root或者leaf的情况）

====================================================================================

题目是给一个n叉树代表一个cluster的nodes。有一个root。每个node只能跟自己的parent或者child通信，root除外，root能收到外部来的message. 这题要求发给root一个信息，让其统计树的node数量并打印出来。要求实现一个Node class，里面的API有receiveMessage(from_node_id, message)和sendMessage(to_node_id, message)。sendMessage不需要implement，可以call到它。默认node一旦被发送message，就会自动执行receiveMessage（）。最开始root会收到receiveMessage(null, message)。follow up：如果系统中会有network failure，就会有retry，那如何保证不重复计算？也就是怎样设计以保证idempotency. 答曰node里加一个set。

====================================================================================

要求实现cd(current_dir, new dir), 返回最终的path, 比如：

cd(/foo/bar, baz) = /foo/bar/baz
cd(/foo/../, ./baz) = /baz
cd(/, foo/bar/../../baz) = /baz
cd(/, ..) = Null
完成以后难度加大，第三个参数是soft link的dictionary，比如：
cd(/foo/bar, baz, {/foo/bar: /abc}) = /abc/baz
cd(/foo/bar, baz, {/foo/bar: /abc, /abc: /bcd, /bcd/baz: /xyz}) = /xyz
dictionary 里有可能有短匹配和长匹配，应该先匹配长的(more specific), 比如：
cd(/foo/bar, baz, {/foo/bar: /abc, /foo/bar/baz: /xyz}) = /xyz
要判断dictionary里是否有循环


================================
coding：ip / cidr iterator

Q1 - 升序iterate 一个 ip address

Q2 - 降序iterate 一个 ip address

Q3 - iterate 一个cidr
Write a program to support the following functionalities:

Ascending iteration of IP addresses: Given a starting IP address and a count, output the next few IP addresses starting from that address.
Descending iteration of IP addresses: Given a starting IP address and a count, output the previous few IP addresses in reverse order.
Iterate over a CIDR range: Given an IP address in CIDR notation, output all the IP addresses within that range.
Input
A command string specifying the function (e.g., "ascending", "descending", "cidr").
An IP address string.
An integer count (valid only for ascending and descending).
Output
A list of IP addresses representing the iterated addresses from the specific IP.
Examples
Input: "ascending", "192.168.1.1", 3 Output: ["192.168.1.1", "192.168.1.2", "192.168.1.3"]

Input: "descending", "192.168.1.3", 2 Output: ["192.168.1.3", "192.168.1.2"]

Input: "cidr", "192.168.1.0/30" Output: ["192.168.1.0", "192.168.1.1", "192.168.1.2", "192.168.1.3"]



====================================================================================

You need to implement two functions:

Implement a node_to_string function that takes an object representing a node and converts it to a string. The node object contains id and value attributes. The function should return a string in the format Node(id=<id>, value=<value>).

Write an infer_function_return_type function that infers and returns the name of the return type of a given function object. You should consider common return types such as int, str, list, and custom objects.

Use assert for testing, rather than raise exception.

Example:

Example 1:

Input:

class Node:
    def __init__(self, id, value):
        self.id = id
        self.value = value

def some_function():
    return 42
Output:

Node(id=1, value=hello)
int
Constraints:

The number of input node objects does not exceed 10.
There is no complexity limit for function objects.

================================
https://www.1point3acres.com/bbs/thread-1161561-3-1.html

VO面了一轮coding 这轮coding发现地里的信息很少，LZ在这里po一下。类似栗抠 饵拔揪 问多少天后感染结束

第四問:  Now, imagine the grid is enormous (billions of cells), but only a sparse set of cells are initially infected. How would you scale this?

前两问都是输出多少轮后epidemic会结束，只是第二问加入了免疫状态。免疫状态的无法被感染，其实很简单，处理时skip掉就行了

第三问是第二问的基础再加一个条件：D天后被感染的会变成免疫，所以最后grid上要么是免疫的要么是未感染的，问多少天到这个状态

前两问其实就是此道粒筘，但是一共五问。开放爱面试只讲快不讲efficient，所以LZ强烈推荐大家代码怎么简单怎么来，不要被LC思维局限了

================================
memory allocator:

allocator(size=1000)
malloc(size) -> pointer
free(pointer) -> bool


memory allocator原题，先提议linked list solution，o(n) time complexity。面试官不是满意，要求brainstorm better solution。之前没准备过，当场brainstorm，最后给个o(logn) time complexity的solution，思路是用sort list of free block （sort by free size）+ linked list。sorted list查找free block，linkedlist做book keeping。

================================
Your goal is to implement a type system for a custom Toy Language. This system must handle primitive types, generics, nested tuples, and function signatures. You will implement the core data structures and a Type Inference engine that substitutes generics with concrete types.

Type Definitions
    Primitives: Lowercase strings like int, float, str, bool
    Generics: Uppercase letters followed by numbers, e.g., T1, T2
    Tuples: Comma-separated types inside brackets, which can be nested. Example: [int, [T1, str]].
    Functions: Defined by a list of parameter types and a single return type. Syntax: [param1, param2] -> returnType.

Part 1: Implement to_str in Node and Function.

Node Class: Represents a type node. It can be a leaf (primitive/generic) or a tuple (list of child nodes).

class Node:
    def  __init__(self, node_type):

        If node_type is a string: It is a primitive or generic.

        If node_type is a list: It is a tuple containing other Node objects.

    def to_str(self):

        Primitives/Generics: Return the string name (e.g., "int").

        Tuples: Return bracketed, comma-separated types (e.g., "[int,T1]").

class Function:
    def    __init__(self, parameters, output_type):
        Represents a function signature.
        parameters: A List[Node] objects.

        output_type: A single Node object.

    def to_str(self):

        Format: (param1,param2,...) -> returnType.

        Example: (int,T1) -> [T1,str]

Part 2: Implement a function get_return_type(parameters, function) that determines the concrete return type of a function based on provided arguments.

def get_return_type(parameters: List[Node], function: Function) -> Node:
    pass

Requirements:
    Generic Resolution: Build a mapping (substitution table) by comparing the Function's expected parameters to the actual parameters provided.

    Substitution: Recursively replace all generics in the function's output_type with the concrete types found during resolution.

    Error Handling:
        Argument Count Mismatch: Raise an error if the number of arguments doesn't match.
        Type Mismatch: Raise an error if a concrete type (e.g., int) is expected but a different type (e.g., str) is provided.
        Generic Conflict: Raise an error if the same generic (e.g., T1) is bound to two different concrete types in the same call.

Test Examples
Example 1: Basic Substitution

    Function: [T1, T2, int, T1] -> [T1, T2]
    Arguments: [int, str, int, int]
    Logic: T1 maps to int, T2 maps to str.
    Result: [int, str]

Example 2: Nested Tuples & Complex Generics

    Function: [[T1, float], T2, T3] -> [T3, T1]
    Arguments: [[str, float], [int, str], int]
    Logic: * T1 is extracted from the first tuple as str.
        T2 maps to the tuple [int, str].
        T3 maps to int.

    Result: [int, str]

Example 3: Conflict Error
    Function: [T1, T1] -> T1
    Arguments: [int, str]
    Error: T1 cannot be both int and str.
================================
大概意思是 要实现一个social network 的class 可以生成snapshot 然后用户相互关注. 然后问用户在某一个snapshot 里是不是 关注了彼此

第一问是 实现 类似于

social_network = SocialNetwork()

social_network.add_user("A")

social_network.add_user("B")

social_network.follow("A", "B")

snapshot = social_network.create_snapshot()

assert snapshot.is_follow("A", "B")

第二问是返回follower 和followee 的名单

第三问是给某个用户推荐关注对象。做法是从这个用户已经关注的人出发，查看这些人各自关注了谁，在这些候选人中，统计哪些人被该用户的关注者关注得次数最多，最后选出关注次数最多的前 k 个作为推荐结果

是需要支持历史的状态查询的 比如查询 id 为 1 的 snapshot 里面两个用户是不是互相关注

================================

题目是一个 n*m 的格子，格子里的人可能被感染了。如果有 N 个邻居被感染，健康的人也会被感染。问题是过多久所有人会被感染。


================================
怪兽对战

模拟两队怪兽之间的回合制战斗，并且必须生成一份详细的战斗日志。

游戏设定


    队伍：有两个队伍（Team A 和 Team B），每个队都有一个有序的怪兽列表。

    怪兽属性：
        name (string)
        health (HP, 正整数)
        attack (攻击力, 正整数)

    终止条件：一直打到其中一个队没有活着的怪兽为止。

核心规则


1. 谁出战？(Active Monster)

    每个队排在最前面、且 health > 0 的怪兽就是当前的 Active Monster。
    如果一个队里没活人了，直接判负。

2. 回合流程 每一个 Round 的流程是固定的：

    Team A 的当前怪兽攻击 Team B 的当前怪兽。
    如果 Team B 的怪兽还没死，那么 Team B 的怪兽立刻反击 Team A 的怪兽。
    下一轮 (Next Round) 开始。

3. 攻击机制

    伤害计算很简单：被打者 HP -= 攻击者 Attack。

    死亡判定：如果 HP <= 0，该怪兽判定死亡。

    补位：死掉的怪兽下场，队里下一个活着的怪兽在下一回合自动成为 Active Monster。

4. 胜负判定

    胜负：当某队存活怪兽数量为 0，另一队获胜。


    平局：如果两个队最后一个怪兽在同一个 Round 里同归于尽（比如 A 把 B 打死，B 临死反击把 A 也带走），则判定为 Draw。

输出要求
你需要输出一个按时间顺序的日志，必须包含：

    每次攻击细节：谁打谁、造成多少伤害、被打者 HP 变化（打之前 -> 打之后）。

    死亡事件：哪个怪兽挂了，哪个新怪兽补位了。

    最终结果："Team A wins", "Team B wins", 或者 "Draw"。

实现要求

代码需要体现良好的 OOD，不要把所有逻辑写在一坨。建议设计以下 Class：

    Monster 类
    Team 类
    BattleEngine 类（用来跑主循环、生成 Log、返回结果）

最后返回一个 BattleResult 对象，包含 winner 和 日志。

写起来不难。要练的话可以思考一下怎么implement一个宝可梦对战的简易python版本。 小tips就是熟悉一下python的fstring，打log会很方便。会问到比如技能威力不一样咋办，技能属性克制怎么搞之类的。
可以谷歌搜一下宝可梦对战属性克制之类的，讲的肯定比我回帖子详细。题目和宝可梦的对战系统基本上是一模一样的


================================
Forward (正向迭代) 要求实现一个 IPV4Iterator 类。 给定一个起始 IP 字符串（比如 "192.168.0.1"），需要让这个迭代器从该起点开始，逐个返回 (yield) 后续所有的 IP 地址，直到达到 IPv4 的上限 255.255.255.255 为止。

代码接口要求： 需要实现标准的 Python 迭代器协议 (Iterator Protocol)，即 __init__, __iter__, 和 __next__。

Part 2: Reverse (反向迭代) 给定一个终点 IP（比如 "192.168.0.255"），实现一个倒序迭代器。要求从这个 IP 开始，反向遍历 (decrement) 直至 0.0.0.0。

Part 3: CIDR (网段解析)：这次的输入是一个 CIDR 格式的字符串（比如 "192.168.1.0/24"）。 你需要解析该字符串，利用位运算 (Bitwise Operation) 算出该网段的 Start IP 和 End IP，然后遍历并返回该网段内包含的所有 IP 地址。

    CIDR 格式说明：IP地址/数字（例如 192.168.1.0/24）。

        斜杠前：基准 IP。

        斜杠后 (/n)：网络前缀长度。意思是该 IP 的 32 位二进制中，前 n 位固定，剩下的 32-n 位（主机位）是可变的。

核心考点是如何根据掩码位（Mask，即 /24）快速计算出网络区间的范围。

Part 4: Optimization 后续问：如何优化时间复杂度和空间复杂度


================================

收到VO coding的提示词，说要熟悉迭代器，生成器和协程。求问有人遇到过吗？

我猜应该是可恢复迭代器那道题，不过在地里也见过IP迭代器的题，不知道是哪一道？


================================

system design

Design webhook
The objective is to design a Webhook service which allows users to register their callback address and an eventId. Whenever eventId is triggered, the system should call the registered callback address with a specific payload. It is safe to assume that each eventId → callback address is unique and one eventId will trigger only one callback address. Assume a highly scalable webhook delivery system which can handle up to 1B events per day




Design places of interest (yelp, foursquare like) service
There are registered places of interest with a particular location (longitude + latitude) and place type (restaurant, hotel, park etc). It is required to support users running query to find N places nearby a particular location and specific place type. Should be able to support low latency on search queries at a very high scale (think 100Ms places across the world).




Design slack
Implement a Slack-like chat system which supports sending messages to a group of chats or to an individual user. Should support use cases to create a new group chat, add users to a group chat, send notifications to a group chat or an individual user. Add support for rich media (image etc) in a chat message. As a follow up, how to implement deletion of a message by user.

focused on large channel fan-out challenge; how notifications (notification on unread messages, notification on message push when users are offline) work; how db can scale up.
Slack, 面试官一直complain 面试太多了，他说看出来我准备过，上来就deep dive 各种multi-tenant 和 global deploy的问题




Design CI/CD workflow system
Design a distributed CI/CD workflow system similar to GitHub Actions that automatically triggers and manages predefined workflows in response to repository events. The system should handle the complete lifecycle from event detection through workflow execution to status reporting.

For example: A Git commit event will trigger some pre-defined workflows. The user triggers a create workflow request through gitpush. The config of this workflow is in the github repository. The id of the github repository can be obtained by analyzing gitpush through the internal api service. Then the create workflow job will be scheduled.

一个github action，主要会focus在怎么schedule，worker怎么setup，vfs怎么mount，怎么做snapshot failover，怎么stream log。

Design a multi tenant CI/CD system which schedules and executes user defined workflows in response to git pushes. The system receives information about pushes via API calls from an internal service which contain the repository id and the current state of the repository (commit hash). Workflows are a sequence of jobs which are defined within a single YAML file in a static location for each repository. Users should be able to view the output and status of jobs as they are running.

这轮面试，面试官深挖了如何实时在 status service 上向用户 stream 每个 job 的 output log。这点我感觉没有答得非常好。




Design payment processor
Design a simple service architecture of a payment processor.
Payment processing is the act of receiving payment requests, forwarding those requests to downstream processors, and then returning responses.

https://www.1point3acres.com/bbs/thread-1131721-1-1.html




Design a system to provide remote devbox in the cloud on demand


Design 在线IDE, 一开始只需要editor，deep dive往dev box方向走


system design： design online chess.com game

sd：设计一个sandbox cloud ide，类似colab。focus在怎么管理虚拟机，stream log 之类的。整体来说比较简单。


SD
crossword puzzle 题目没说board大小，面试官说可以想象成中等尺寸：50*50. 有100个word slot要填，字典是1m，只要找出任意一个组合就可以。这题目真的难啊。证明完单机不行，就开始设计多机版本。我讲了一个利用stochastic optimization的方法，面试全程就是给面试官讲明白E2E 的流程， 也没其他deep dive。面试官最后来了一句：第一次见这个方法，要回去验证一下。我觉得完了，挂了。所以问了面试官具体怎么做。最正确的答案是 distributed dfs， 但他没见人答过，也不指望大家答这个。 大部人都是用 stimulation 法，分任务，死胡同的时候再分情况。

https://www.1point3acres.com/bbs/thread-1156374-1-1.html 
OAI 店面碰到了一个奇葩SD。让设计一个service来解crossword puzzle. 输入是这样的一个board，和一个dictionary:
Words:
- TIRE: A Wheel on a car
- RATS: Small Rodents
- BATH: A tub to clean
- BE: To, or not to
output是：
1 Down: A wheel on a car
2 Across: Small rodents
3 Across: To, or not to
3 Down: A tub to clean in
开始的时候蒙了一下，当algorithm题来处理了，先花了很多时间试图找到最优算法。后来意识到这题不是在考算法，就是暴力找。但当时心态有点崩了，试图套那个functional requirements, non-functional requirements, high level design, etc的模版，后来想想也是个错误。


这个题目本质上应该就是一个job scheduler的问题。当board很大的时候，在一台机器/Core上暴力算是没法按时解决。所以需要把这个问题分解到不同的worker上。怎么分解请自己发挥。这里需要注意的是有些机器的任务可能很快就走到死胡同了，而有些task可能过大，所以还需要动态split task and redistribute.



Design a calendar like apple calendar，用户要能够创建event，然后有day，month，year的view，之后的followup说要support multi device，就是一个设备创建了以后要同步到另一个设备


SD: short URL

================================

HM chat
- Why OpenAI?

- What are the risks to AGI?

- How would you decide if a newer model could be released or not?

- How have you come to your current place in your career?

- 其他常规behavioral

- BQ with manager：why openai，what's your view on AGI，然后一些经典negative questions。
- 常规问题，怎么看AI safety之类的
- Behavior 也很神奇，开始以为开放爱是比较standard 的behavior，然而 花了30分钟问我对AI safety的看法，从各种角度，对人类工作的影响，对经济的影响之类的。


- BQ：就是常规 BQ。个人感觉他们最看重两点：

- 你为什么想来这家公司?
- 你的 mission 动机跟公司使命到底有多对齐？


Project deepdive
- 自己挑了一个既有深度，又有广度，还有 practical tradeoff 的项目。经过好几家其他公司的捶打，感觉这一轮是最稳的。

[def]: image.png







