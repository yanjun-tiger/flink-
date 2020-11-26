package com.atguigu.app;

import com.atguigu.bean.ApacheLog;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 * @author zhouyanjun
 * @create 2020-11-25 21:24
 */
public class HotUrlApp {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本文件创建流,转换为JavaBean并提取时间戳
//        SingleOutputStreamOperator<ApacheLog> apachLogDS = env.readTextFile("input/apache.log")
        SingleOutputStreamOperator<ApacheLog> apachLogDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss"); //用来解析时间，首先传入匹配的时间模式
                    Date parse = sdf.parse(fields[3]);
                    long time = parse.getTime(); //Returns the number of milliseconds since January 1, 1970, 00:00:00 GMT。就是返回时间戳格式
                    return new ApacheLog(fields[0], fields[1], time, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

    }
}
