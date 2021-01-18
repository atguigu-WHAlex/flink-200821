package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取文件数据
        DataSource<String> input = env.readTextFile("input");

        //3.压平
        FlatMapOperator<String, String> wordDS = input.flatMap(new MyFlatMapFunc());

        //4.将单词转换为元组
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS = wordDS.map((MapFunction<String, Tuple2<String, Integer>>) value -> {
            return new Tuple2<>(value, 1);
            //return Tuple2.of(value, 1);
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        //5.分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDS.groupBy(0);

        //6.聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);

        //7.打印结果
        result.print();

    }

    //自定义实现压平操作的类
    public static class MyFlatMapFunc implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            //按照空格切割
            String[] words = value.split(" ");
            //遍历words,写出一个个的单词
            for (String word : words) {
                out.collect(word);
            }
        }
    }

}
