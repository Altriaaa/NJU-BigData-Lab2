#!/bin/zsh

setopt +o nomatch

# 定义变量
JAR_FILE="target/Lab2-1.0-SNAPSHOT.jar"
MAIN_CLASS="Main"
LOCAL_INPUT="src/test/Input"
LOCAL_OUTPUT="src/test/Output"
HDFS_INPUT="/Input"
HDFS_OUTPUT="/Output"
OPTIONS=""

# 选择操作模式
echo "请选择操作模式:"
echo "1) 倒排索引 (默认)"
echo "2) TF-IDF 计算"
read operation_mode

if [[ $operation_mode == "2" ]]; then
    OPTIONS="-t"
    echo "已选择 TF-IDF 模式"
else
    echo "已选择 倒排索引 模式"
    echo "是否执行排序? (y/n)"
    read test_sort
    if [[ $test_sort == "y" || $test_sort == "Y" ]]; then
        OPTIONS="-s"
        echo "已启用排序"
    fi
fi

echo "=== 开始执行测试 ==="

# 步骤1: 清理HDFS上现有目录
echo "清理HDFS目录..."
hadoop fs -rm -r ${HDFS_INPUT} ${HDFS_OUTPUT} ${HDFS_OUTPUT}_sorted 2>/dev/null

# 步骤2: 创建HDFS输入目录
echo "创建HDFS输入目录..."
hadoop fs -mkdir -p ${HDFS_INPUT}

# 步骤3: 上传测试文件到HDFS
echo "上传测试文件到HDFS..."
hadoop fs -put ${LOCAL_INPUT}/* ${HDFS_INPUT}/

# 步骤4: 运行MapReduce作业
echo "运行MapReduce作业..."
hadoop jar ${JAR_FILE} ${MAIN_CLASS} ${OPTIONS} ${HDFS_INPUT} ${HDFS_OUTPUT}

# 步骤5: 准备本地输出目录
echo "准备本地输出目录..."
rm -rf ${LOCAL_OUTPUT}
mkdir -p ${LOCAL_OUTPUT}

# 步骤6: 将结果下载到本地
if [[ $operation_mode != "2" && -n "${OPTIONS}" ]]; then
    # 倒排索引模式 + 排序
    echo "下载排序结果到本地..."
    hadoop fs -get ${HDFS_OUTPUT}_sorted/* ${LOCAL_OUTPUT}/
else
    # TFIDF模式或倒排索引模式（无排序）
    echo "下载结果到本地..."
    hadoop fs -get ${HDFS_OUTPUT}/* ${LOCAL_OUTPUT}/
fi

# 合并结果并显示
cat ${LOCAL_OUTPUT}/part-* | column -t -s $'\t' > ${LOCAL_OUTPUT}/result.txt


echo "=== 测试完成! 结果保存在 ${LOCAL_OUTPUT} ==="
echo "结果预览:"
cat ${LOCAL_OUTPUT}/result.txt | head -10