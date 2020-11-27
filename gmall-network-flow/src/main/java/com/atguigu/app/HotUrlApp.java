package com.atguigu.app;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.UrlViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.ListValue;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.mutable.StringBuilder;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;

/**
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 * @author zhouyanjun
 * @create 2020-11-25 21:24
 */
public class HotUrlApp {
    public static void main(String[] args) throws Exception {
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
                    long time = sdf.parse(fields[3]).getTime();//Returns the number of milliseconds since January 1, 1970, 00:00:00 GMT。就是返回时间戳格式
                    return new ApacheLog(fields[0], fields[1], time, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

        //3 先按照get过滤数据（确定是访问网页）,按照url分组,开窗,累加计算
        OutputTag<ApacheLog> outputTag = new OutputTag<ApacheLog>("sideOutPut"){};

        SingleOutputStreamOperator<UrlViewCount> aggregate = apachLogDS.filter(data -> "GET".equals(data.getMethod()))
                .keyBy(data -> data.getIp()).timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .sideOutputLateData(outputTag)
                //聚合函数reduce是输入输出类型要求一致，aggregate输入输出类型不一致；
                // 窗口函数
                .aggregate(new UrlCountAggFunc(), new UrlCountWindowFunc());

        //4 按照窗口结束时间进行分组。并计算组内排序
        SingleOutputStreamOperator<String> result = aggregate.keyBy(data -> data.getWindowEnd())
                .process(new UrlCountProcessFunc(5));

        //5 打印数据
        result.print("result");
        aggregate.getSideOutput(outputTag).print("outputTag");
        //6 执行
        env.execute();
    }

    //<IN, ACC, OUT>
    public static class UrlCountAggFunc implements AggregateFunction<ApacheLog, Long, Long> {

        @Override
        //创造中间状态。初始化
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        //计算每一条过来的数据
        public Long add(ApacheLog value, Long accumulator) {
            return accumulator+1L;
        }

        @Override
         //获取结果
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        //这个一般是session窗口使用
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    //<IN, OUT, KEY, W extends Window>
    public static class UrlCountWindowFunc implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        //是要封装到urlViewCount类中
        //我需要获取到每个窗口的结尾时间戳
        public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            long end = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new UrlViewCount(s,end,count));
        }
    }

    //<K, I, O>
    private static class UrlCountProcessFunc extends KeyedProcessFunction<Long,UrlViewCount,String> {
        //定义属性
        private Integer topsize;

        public UrlCountProcessFunc() {
        }
        //有参构造，初始化属性
        public UrlCountProcessFunc(Integer topsize) {
            this.topsize = topsize;
        }

        //我需要集合状态，来保存每一条数据。要进入心流状态。
        //<V extends Value> 状态也需要泛型
        private ListState<UrlViewCount> listState;

        @Override
        //需要生命周期函数
        public void open(Configuration parameters) throws Exception {
            //什么叫获得状态的句柄？ 汪辉说这个是初始化。
            listState =getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("list-state",UrlViewCount.class));
        }

        @Override
        //处理每一条数据
        //需要计算组内排序。等到一个窗口的所有数据在状态中保存完整了，就可以进行计算。此时，需要注册计时器。
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            //将数据保存到状态中
            listState.add(value);
            //注册定时器，当一个窗口结束，处理保存在状态中的一个窗口的数据
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1L);

            //注册定时器,用于触发清空状态的
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60000L);

        }

        @Override
        //到时间之后启动定时器。开始对窗口中的数据进行排序
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if(timestamp== ctx.getCurrentKey() + 60000L){
                //清空状态
                listState.clear();
                return; //跳出整个方法，下面的代码不会执行
            }

            //1.要取出状态中的数据
            Iterable<UrlViewCount> urlViewCounts_iterable = listState.get();
            Iterator<UrlViewCount> iterator = urlViewCounts_iterable.iterator();
            ArrayList<UrlViewCount> urlViewCounts = Lists.newArrayList(iterator);

            //2.排序
            urlViewCounts.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    if (o1.getCount() > o2.getCount()) {
                        return -1;
                    } else if (o1.getCount() < o2.getCount()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            StringBuilder sb = new StringBuilder();
            sb.append("======================\n");
            sb.append("当前窗口结束时间为:").append(new Timestamp(timestamp - 1L)).append("\n");

            //取前topSize条数据输出
            for (int i = 0; i < Math.min(topsize, urlViewCounts.size()); i++) {
                //取出数据
                UrlViewCount itemCount = urlViewCounts.get(i);
                sb.append("TOP ").append(i + 1);
                sb.append(" URL=").append(itemCount.getUrl());
                sb.append(" 页面热度=").append(itemCount.getCount());
                sb.append("\n");
            }
            sb.append("======================\n\n");

//            listState.clear();
            Thread.sleep(1000);
            //输出数据
            out.collect(sb.toString());
        }
    }
}
