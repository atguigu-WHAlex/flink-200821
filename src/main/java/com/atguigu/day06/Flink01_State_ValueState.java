package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_State_ValueState {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        //3.按照传感器ID分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        //4.使用RichFunction实现水位线跳变报警需求
        keyedStream.flatMap(new RichFlatMapFunction<WaterSensor, String>() {
            //定义状态
            private ValueState<Integer> vcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state", Integer.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {

                //获取状态中的数据
                Integer lastVc = vcState.value();

                //更新状态
                vcState.update(value.getVc());

                //当上一次水位线不为NULL并且出现跳变的时候进行报警
                if (lastVc != null && Math.abs(lastVc - value.getVc()) > 10) {
                    out.collect(value.getId() + "出现水位线跳变！");
                }
            }
        }).print();

        //5.执行任务
        env.execute();

    }

}
