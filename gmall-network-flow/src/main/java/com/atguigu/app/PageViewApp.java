package com.atguigu.app;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Int;

/**
 * 实现一个网站总浏览量的统计。我们可以设置滚动时间窗口，实时统计每小时内的网站PV。
 *
 * @author zhouyanjun
 * @create 2020-11-26 13:35
 */
public class PageViewApp {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从文件读取数据创建流并转换为JavaBean同时提取事件时间
        SingleOutputStreamOperator<UserBehavior> userDS = env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] fileds = line.split(",");
                    return new UserBehavior(Long.parseLong(fileds[0]),
                            Long.parseLong(fileds[1]),
                            Integer.parseInt(fileds[2]),
                            fileds[3],
                            Long.parseLong(fileds[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.按照"pv"过滤,按照itemID分组,开窗,计算数据  就是个wordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = userDS.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("pv", 1);
                    }
                })
                //这样写，本身key是什么类型，key就是什么类型 // 同一个key一定在一个分区，所以产生了数据倾斜问题
                .keyBy(data->data.f0)//括号里用0也行。使用KeySelector的方式，就可以保留对应的类型。如果是"字段名"，那就是个元组类型，看源码可知。   The KeySelector to be used for extracting the key for partitioning
                .timeWindow(Time.hours(1))
                .sum(1);

        //4.打印输出
        pv.print();

        //5.执行
        env.execute();

    }
}
