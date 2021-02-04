package com.atguigu.day12;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

public class FlinkSQL06_Function_UDTAF {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //3.将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        //4.先注册再使用
//        tableEnv.createTemporarySystemFunction("myavg", MyAvg.class);

        //TableAPI

        //6.执行任务
        env.execute();

    }

    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, String>, VcTop2> {

        @Override
        public VcTop2 createAccumulator() {
            return new VcTop2();
        }

        public void accumulate(VcTop2 acc, Integer value) {

        }

        public void emitValue(VcTop2 acc, Collector<Tuple2<Integer, Integer>> out) {

        }


    }


}
