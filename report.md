# Lab2 实验报告

| 学院       | 学号      | 姓名   |
| :--------- | :-------- | :----- |
| 计算机学院 | 221220046 | 宋承柏 |

## 任务1：倒排索引+排序

### 设计思路与代码

模块划分：
 <img src="C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329210726812.png" alt="image-20250329210726812" style="zoom:80%;" />

任务1被分为`InvertedIndex`和`Sorter`两个模块完成，统一由Main类调用，各设置Driver类为Hadoop job的入口。

以下描述设计思路，本部分主要代码在**任务3**一并给出。

#### 主要处理流程

##### Map阶段 (InvertedIndexMapper)

- 输入: <LongWritable, Text>
  - LongWritable: 文档中行的偏移量
  - Text: 文档的文本行内容
- 输出: <Text, Text>
  - Text(key): 单词
  - Text(value): 文件名

##### Combiner阶段 (InvertedIndexCombiner)

- 输入: <Text, Text>
  - 与Mapper输出相同
- 输出: <Text, Text>
  - Text(key): 单词
  - Text(value): 格式化的字符串，形如 "filename:count;filename:count"

Combiner作为本地聚合器，在数据传输到Reducer前先在Map端预聚合，减少网络传输量。

##### Reduce阶段 (InvertedIndexReducer)

- 输入: <Text, Iterable<Text>>
  - Text(key): 单词
  - Iterable<Text>(values): 包含所有文件计数信息的列表
- 输出: <Text, Text>
  - Text(key): 格式化的单词，形如 "[word]"
  - Text(value): 平均频率和文件计数对，格式为 "avg, filename:count; filename:count"

Reduce阶段计算每个单词在所有文档中的统计信息，包括平均频率和在各文档中的出现次数。

##### Map阶段 (SorterMapper)

- 输入: <LongWritable, Text>
  - LongWritable: 文件行偏移量
  - Text: 倒排索引结果行，格式为 "[word] avg, filename:count; filename:count"
- 输出: <DoubleWritable, Text>
  - DoubleWritable(key): 从结果中提取的平均频率，用于排序
  - Text(value): 原始的完整行内容

Map阶段解析倒排索引模块的输出行，提取每个单词的平均频率作为排序键，并保留完整行内容。利用DoubleWritable的自然排序特性实现数值降序排列。

##### Reduce阶段 (SorterReducer)

- 输入: <DoubleWritable, Iterable<Text>>
  - DoubleWritable(key): 平均频率（已排序）
  - Iterable<Text>(values): 同一平均频率对应的所有行
- 输出: <Text, Text>
  - Text(key): 单词，恢复为 "[word]" 格式
  - Text(value): 统计信息，保持 "avg, filename:count; filename:count" 格式

Reduce阶段将排序后的结果转换回原始格式，保持数据内容不变，仅调整顺序。

### 输出结果

在平台上使用如下命令：
`/home/gr20/Lab2-1.0-SNAPSHOT.jar Main -s /user/root/Exp2 /user/gr20/Output`

倒排索引结果保存在`/user/gr20/Output`，排序后结果保存在`/user/gr20/Output_sorted`。

平台查看部分结果：

 <img src="C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329180540567.png" alt="image-20250329180540567" style="zoom:80%;" />

本地结果对比：

<img src="C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329181011150.png" alt="image-20250329181011150" style="zoom:80%;" />

### Yarn执行报告

Job1: Inverted Index:

![image-20250329181101495](C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329181101495.png)

Job2: Sorter: 

![image-20250329181149430](C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329181149430.png)



## 任务2：计算TFIDF

### 设计思路

Map阶段，读入的是<行号：line>，在Mapper内部使用一个哈希表，记录每个单词以及其计数，这样就形成了在单文件内单行的TF。同时，通过context获取当前文件名，在输出时，将文件名与计数拼接，输出<word, filename#cnt>。这一结果被发送给Reduce，reducer获取到一系列的<word, filename#cnt>，再使用一个哈希表，记录某个filename及word在其内的计数。于是，哈希表内的Value就是该word在某个filename内的TF，而哈希表的size就是包含该word的文档数。然后就可以输出结果。

#### Map阶段 (TFIDFMapper)

- 输入: <LongWritable, Text>
  - LongWritable: 文档中行的偏移量
  - Text: 文档的文本行内容
- 输出: <Text, Text>
  - Text(key): 单词
  - Text(value): 格式为 "filename#count"，表示该单词在特定文档中的出现次数

Map阶段实现了两个关键功能： 

1. 过滤停用词文件(cn_stopwords.txt)，确保其不参与统计
2. 计算每个单词在单个文档中的出现频率(TF)

特别地，该设计使用cleanup方法在处理完整个文档后输出结果，而不是每次map都发送。减少了传输开销。

#### Reduce阶段 (TFIDFReducer)

- 输入: <Text, Iterable<Text>>
  - Text(key): 单词
  - Iterable<Text>(values): 该单词在各个文档中的出现信息列表
- 输出: <Text, Text>
  - Text(key): 文档名
  - Text(value): 格式为 "word\tTF-IDF"

Reduce阶段完成了TF-IDF的核心计算。

### 主要代码

```java
public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private Text wordKey = new Text();
    private Text docInfo = new Text();
    private String filename;
    private HashMap<String, Integer> wordCnt = new HashMap<>();
    private static final String STOPWORDS_FILE_NAME = "cn_stopwords.txt";
    
    /**
     * 初始化Mapper，获取当前文件名
     * @param context 上下文对象，包含配置信息
     */
    @Override
    protected void setup(Context context)
    {
        filename = ((FileSplit) context.getInputSplit()).getPath().getName();
    }

    /**
     * 处理每一行数据，统计单词出现次数
     * @param key 输入的键，表示行号
     * @param value 输入的值，表示行内容
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 如果当前文件是停用词文件，则跳过处理
        if(STOPWORDS_FILE_NAME.equals(filename))
        {
            return;
        }
        StringTokenizer itr = new StringTokenizer(value.toString());
        while(itr.hasMoreTokens())
        {
            String word = itr.nextToken();
            wordCnt.put(word, wordCnt.getOrDefault(word, 0)+1);
        }
    }

    /**
     * 在Mapper结束时输出每个单词及其在当前文件中的出现次数
     * @param context 上下文对象，用于输出结果
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        // 如果当前文件是停用词文件，则不输出任何内容
        if(STOPWORDS_FILE_NAME.equals(filename))
        {
            return;
        }
        for(Map.Entry<String, Integer> entry : wordCnt.entrySet())
        {
            wordKey.set(entry.getKey());
            docInfo.set(filename + "#" + entry.getValue());
            context.write(wordKey, docInfo);
        }
    }
}
```

```java
/**
 * TF-IDF计算的Reducer类
 * 负责汇总单词在各文档中的出现频率，并计算TF-IDF值
 * 输入: <单词, 文档名称#词频>
 * 输出: <文档名称, 单词\tTF-IDF值>
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, Text>
{
    private Text resKey = new Text();
    private Text resVal = new Text();
    private int docCnt;
    private HashMap<String, Integer> docInfo = new HashMap<>();

    /**
     * 初始化Reducer，从配置中获取文档总数
     * @param context 上下文对象，包含配置信息
     */
    @Override
    protected void setup(Context context)
    {
        docCnt = context.getConfiguration().getInt("docCnt", 1);
    }

    /**
     * 计算单词的TF-IDF值并输出结果
     * @param key 输入的键，表示单词
     * @param values 输入的值列表，每个值表示单词在某个文档中的出现次数
     * @param context 上下文对象，用于输出结果
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        docInfo.clear();
        // 统计单词在各个文档中的出现次数
        for (Text val : values)  // val 形如 "filename#cnt"
        {
            String[] info = val.toString().split("#");
            if (info.length == 2)
            {
                int cnt = Integer.parseInt(info[1]);
                docInfo.put(info[0], docInfo.getOrDefault(info[0], 0) + cnt);
            }
        }
        int relatingDocNum = docInfo.size();
        double idf = Math.log10((double) docCnt / (relatingDocNum+1));
        // 计算每个文档中该单词的TF-IDF值并输出
        for(Map.Entry<String, Integer> entry : docInfo.entrySet())
        {
            String filename = entry.getKey();
            int tf = entry.getValue();
            double TF_IDF = tf * idf;
            resKey.set(filename);
            resVal.set(key.toString() + "\t" + String.format("%.2f", TF_IDF));
            context.write(resKey, resVal);
        }
    }
}
```



### 输出结果

在平台上使用如下命令：
`/home/gr20/Lab2-1.0-SNAPSHOT.jar Main -t /user/root/Exp2 /user/gr20/Output_TFIDF`

结果保存在`/user/gr20/Output_TFIDF`

平台查看部分结果：

 <img src="C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329202034516.png" alt="image-20250329202034516" style="zoom:80%;" />

本地结果对比：

 <img src="C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329201637116.png" alt="image-20250329201637116" style="zoom:67%;" />

### Yarn执行报告

![image-20250329202228898](C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329202228898.png)



## 任务3：过滤停用词

### 设计思路

在`InvertedIndexMapper`中加上对停用词文件的判断即可。使用一个HashSet记录从文件中读到的停用词，当检测到的token与其中的词匹配时，跳过该token的记录。

### 主要代码

```java
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private Text word = new Text();
    private Text filename = new Text();
    private Set<String> stopwords = new HashSet<>();
    private String currentFileName;
    private static final String STOPWORDS_FILE_NAME = "cn_stopwords.txt";

    /**
     * 在Mapper的setup方法中读取停用词文件
     * @param context 上下文对象
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
        // 获取当前文件的名称
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        currentFileName = fileSplit.getPath().getName();
        filename.set(currentFileName);
        // 检查是否有停用词文件路径
        String stopwordsPath = context.getConfiguration().get("stopwordsPath");
        if (stopwordsPath != null && !stopwordsPath.isEmpty())
        {
            // 读取停用词文件
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(stopwordsPath);
            if (fs.exists(path))
            {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = reader.readLine()) != null)
                {
                    stopwords.add(line.trim());
                }
                reader.close();
                System.out.println("已加载 " + stopwords.size() + " 个停用词");
            }
            else
            {
                System.err.println("停用词文件不存在: " + stopwordsPath);
            }
        }
    }

    /**
     * 将输入文本分割成单词，并为每个单词创建一个与当前文件名关联的键值对
     * @param key 输入行的偏移量
     * @param value 输入的文本行
     * @param context 输出形式为(word, filename)的键值对
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        if(STOPWORDS_FILE_NAME.equals(currentFileName))
        {
            return; // 如果当前文件是停用词文件，则跳过，确保停用词文件不会被当作输入文件处理
        }
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreElements())
        {
            String token = itr.nextToken();
            // 如果停用词过滤器启用，并且当前单词在停用词列表中，则跳过
            if (!stopwords.isEmpty() && stopwords.contains(token))
            {
                continue;
            }
            word.set(token);
            context.write(word, filename);
        }
    }
}
```

```java
public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
{
    private Text outputValue = new Text();

    /**
     * 对于每个单词，将其在各个文件中出现的次数进行本地合并
     * @param key 单词
     * @param values 出现该单词的文件列表
     * @param context 结果，形如 (key, "filename:cnt;...")
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        HashMap<String, Integer> cnt = new HashMap<>();
        // 统计每个文件(value)中，当前单词(key)出现的次数
        for (Text value : values)
        {
            String filename = value.toString();
            cnt.put(filename, cnt.getOrDefault(filename, 0) + 1);
        }
        // 将统计结果拼接成字符串
        StringBuilder sb = new StringBuilder();
        for (String filename : cnt.keySet())
        {
            sb.append(filename).append(":").append(cnt.get(filename)).append(";");
        }
        if(sb.length() > 0)
            sb.deleteCharAt(sb.length() - 1);

        outputValue.set(sb.toString());
        context.write(key, outputValue);    // 形如 (key, "filename:cnt;...")
    }
}
```

```java
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
{
    private Text res = new Text();
    private static final DecimalFormat df = new DecimalFormat("0.00");

    /**
     * 对于每个单词，将其在各个文件中出现的次数进行合并
     * @param key 单词
     * @param values 单词在各个文件中出现的次数，已经过本地合并
     * @param context 结果，形如 ([key], "avg, filename:cnt; filename:cnt")
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        HashMap<String, Integer> cnt = new HashMap<>();
        int total = 0;
        // 合并统计结果
        for(Text partRes : values)
        {
            String[] entries = partRes.toString().split(";");
            for(String entry : entries)
            {
                String[] parts = entry.split(":");
                if(parts.length == 2)
                {
                    String filename = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    total += count;
                    cnt.put(filename, cnt.getOrDefault(filename, 0) + count);
                }
            }
        }
        // 生成格式化结果
        double avg = !cnt.isEmpty() ? (double)total / cnt.size() : 0;
        StringBuilder output = new StringBuilder();
        output.append(df.format(avg)).append(",").append(" ");
        for(String name : cnt.keySet())
        {
            output.append(name).append(":").append((cnt.get(name))).append(";").append(" ");
        }
        if(output.length() >= 2)
        {
            output.setLength(output.length()-2);
        }
        res.set(output.toString());
        context.write(new Text("[" + key.toString() + "]"), res);
    }
}
```

### 输出结果

在平台上使用如下命令：
`/home/gr20/Lab2-1.0-SNAPSHOT.jar Main -r /user/gr20/Stopwords/cn_stopwords.txt -s /user/root/Exp2 /user/gr20/Output_Filtered`

倒排索引结果保存在`/user/gr20/Output_Filtered`，排序结果保存在`/user/gr20/Output_Filtered_sorted`。

平台查看部分结果：

 <img src="C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329205318890.png" alt="image-20250329205318890" style="zoom:67%;" />

本地结果对比：

![image-20250329203428246](C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329203428246.png)

### Yarn执行报告

Job1: Inverted Index:

![image-20250329205402609](C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329205402609.png)

Job2: Sorter: 

![image-20250329205417188](C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329205417188.png)



## 额外说明

为了将三个任务整合到统一程序中，我额外设计了一个Main类，用于接受输入参数，调用相应的程序，使用方式如下：

![image-20250329215946604](C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329215946604.png)

通过`-s -t -r`三个选项选择要完成的任务。

例如，要执行任务3，可以使用：`hadoop jar target/Lab2-1.0-SNAPSHOT.jar Main -r /Stopwords/cn_stopwords.txt -s /Input /Output`。

另外，为便于调试，我编写了一个shell脚本用于自动化测试流程。它会在工作目录下读取预先设置好的输入目录，将其中内容上传至HDFS，并在测试完成后将输出内容复制到本地目录便于查看。

直接运行即可：

 <img src="C:\Users\勇者无惧\AppData\Roaming\Typora\typora-user-images\image-20250329220448808.png" alt="image-20250329220448808" style="zoom:67%;" />

该脚本使用zsh，若需要在bash上运行，请更改shebang，并将开头的`setopt +o nomatch`改为`shopt -s nullglob`。