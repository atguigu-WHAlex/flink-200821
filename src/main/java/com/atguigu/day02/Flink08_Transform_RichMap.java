package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Transform_RichMap {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.从文件读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor.txt");

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = stringDataStreamSource.map(new MyRichMapFunc());

        //4.打印结果数据
        waterSensorDS.print();

        //5.执行任务
        env.execute();

    }

    //RichFunction富有的地方在于:1.声明周期方法,2.可以获取上下文执行环境,做状态编程
    public static class MyRichMapFunc extends RichMapFunction<String, WaterSensor> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("Open方法被调用！！");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("Close方法被调用！！！");
        }
    }


}
