package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Flink09_Sink_Es {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
//        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.readTextFile("input/sensor.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0],
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]));
                    }
                });

        //3.将数据写入ES
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder =
                new ElasticsearchSink.Builder<WaterSensor>(httpHosts, new MyEsSinkFunc());

        //批量提交参数
        waterSensorBuilder.setBulkFlushMaxActions(1);
        ElasticsearchSink<WaterSensor> elasticsearchSink = waterSensorBuilder.build();

        waterSensorDS.addSink(elasticsearchSink);

        //4.执行任务
        env.execute();
    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<WaterSensor> {

        @Override
        public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {

            HashMap<String, String> source = new HashMap<>();
            source.put("ts", element.getTs().toString());
            source.put("vc", element.getVc().toString());

            //创建Index请求
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor1")
                    .type("_doc")
//                    .id(element.getId())
                    .source(source);

            //写入ES
            indexer.add(indexRequest);
        }

    }

}
