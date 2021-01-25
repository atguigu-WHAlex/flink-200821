package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Flink08_State_Backend {

    public static void main(String[] args) throws IOException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义状态后端,保存状态的位置
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs:hadoop102:8020/flink/ck"));
        env.setStateBackend(new RocksDBStateBackend("hdfs:hadoop102:8020/flink/ck"));

        //开启CK
        env.getCheckpointConfig().enableUnalignedCheckpoints();

    }

}
