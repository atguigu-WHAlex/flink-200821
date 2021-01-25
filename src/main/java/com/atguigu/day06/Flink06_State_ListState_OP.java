package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Flink06_State_ListState_OP {

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

        //3.统计元素的个数
        waterSensorDS.map(new MyMapFunc()).print();

        //4.执行任务
        env.execute();

    }

    public static class MyMapFunc implements MapFunction<WaterSensor, Integer>, CheckpointedFunction {

        //定义状态
        private ListState<Integer> listState;
        private Integer count = 0;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            listState = context
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Integer>("state", Integer.class));

            Iterator<Integer> iterator = listState.get().iterator();

            while (iterator.hasNext()) {
                count += iterator.next();
            }
        }

        @Override
        public Integer map(WaterSensor value) throws Exception {
            count++;
            return count;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            listState.add(count);
        }

    }

}
