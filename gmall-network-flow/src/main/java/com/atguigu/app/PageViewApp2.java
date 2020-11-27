package com.atguigu.app;

import com.atguigu.bean.PvCount;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Random;

/**
 * 实现一个网站总浏览量的统计。我们可以设置滚动时间窗口，实时统计每小时内的网站PV。
 * @author zhouyanjun
 * @create 2020-11-26 14:19
 */
public class PageViewApp2 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(6);
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

        //3.按照"pv"过滤,按照itemID分组,开窗,计算数据
        SingleOutputStreamOperator<PvCount> aggregate = userDS.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>("pv_" + random.nextInt(8), 1); //都是pv，把pv打散。就是分不同的组。1-8中随机一个数
                    }
                })
                .keyBy(0) //按组进行分组
                .timeWindow(Time.hours(1)) //每个组，都进行开窗，进行计算
                .aggregate(new PvCountAggFunc(), new PvCountWindowFunc()); //对于每个组的每一个窗口中的数据，进行聚合计算。想要包装上windowEnd。就不使用sum

//        aggregate.print();
        //2> PvCount(pv=pv_6, windowEnd=1511661600000, count=5196)
        //1> PvCount(pv=pv_3, windowEnd=1511661600000, count=5195)
        //2> PvCount(pv=pv_6, windowEnd=1511665200000, count=6128)
        //1> PvCount(pv=pv_3, windowEnd=1511665200000, count=6009)
        //2> PvCount(pv=pv_6, windowEnd=1511668800000, count=6032)
        //1> PvCount(pv=pv_3, windowEnd=1511668800000, count=5964)
        //2> PvCount(pv=pv_6, windowEnd=1511672400000, count=5657)
        //1> PvCount(pv=pv_3, windowEnd=1511672400000, count=5493)
        //1> PvCount(pv=pv_3, windowEnd=1511676000000, count=6121)
        //2> PvCount(pv=pv_6, windowEnd=1511676000000, count=6067)
        //1> PvCount(pv=pv_3, windowEnd=1511679600000, count=6406)
        //2> PvCount(pv=pv_6, windowEnd=1511679600000, count=6339)
        //1> PvCount(pv=pv_3, windowEnd=1511683200000, count=6556)
        //1> PvCount(pv=pv_3, windowEnd=1511686800000, count=6589)
        //2> PvCount(pv=pv_6, windowEnd=1511683200000, count=6595)
        //1> PvCount(pv=pv_3, windowEnd=1511690400000, count=6081)
        //2> PvCount(pv=pv_6, windowEnd=1511686800000, count=6577)
        //1> PvCount(pv=pv_3, windowEnd=1511694000000, count=2)
        //2> PvCount(pv=pv_6, windowEnd=1511690400000, count=6148)
        //2> PvCount(pv=pv_6, windowEnd=1511694000000, count=1)
        //5> PvCount(pv=pv_4, windowEnd=1511661600000, count=5306)
        //5> PvCount(pv=pv_2, windowEnd=1511661600000, count=5277)
        //5> PvCount(pv=pv_2, windowEnd=1511665200000, count=6006)
        //5> PvCount(pv=pv_4, windowEnd=1511665200000, count=6011)
        //5> PvCount(pv=pv_2, windowEnd=1511668800000, count=5955)
        //5> PvCount(pv=pv_4, windowEnd=1511668800000, count=5919)
        //5> PvCount(pv=pv_4, windowEnd=1511672400000, count=5611)
        //5> PvCount(pv=pv_2, windowEnd=1511672400000, count=5543)
        //5> PvCount(pv=pv_2, windowEnd=1511676000000, count=6052)
        //5> PvCount(pv=pv_4, windowEnd=1511676000000, count=6066)
        //5> PvCount(pv=pv_4, windowEnd=1511679600000, count=6335)
        //5> PvCount(pv=pv_2, windowEnd=1511679600000, count=6259)
        //5> PvCount(pv=pv_2, windowEnd=1511683200000, count=6601)
        //5> PvCount(pv=pv_4, windowEnd=1511683200000, count=6633)
        //5> PvCount(pv=pv_4, windowEnd=1511686800000, count=6527)
        //5> PvCount(pv=pv_2, windowEnd=1511686800000, count=6484)
        //5> PvCount(pv=pv_2, windowEnd=1511690400000, count=5921)
        //5> PvCount(pv=pv_4, windowEnd=1511690400000, count=6052)
        //5> PvCount(pv=pv_2, windowEnd=1511694000000, count=1)
        //5> PvCount(pv=pv_4, windowEnd=1511694000000, count=2)
        //3> PvCount(pv=pv_7, windowEnd=1511661600000, count=5166)
        //3> PvCount(pv=pv_1, windowEnd=1511661600000, count=5230)
        //3> PvCount(pv=pv_0, windowEnd=1511661600000, count=5211)
        //3> PvCount(pv=pv_5, windowEnd=1511661600000, count=5309)
        //3> PvCount(pv=pv_5, windowEnd=1511665200000, count=5997)
        //3> PvCount(pv=pv_7, windowEnd=1511665200000, count=5904)
        //3> PvCount(pv=pv_0, windowEnd=1511665200000, count=6026)
        //3> PvCount(pv=pv_1, windowEnd=1511665200000, count=5941)
        //3> PvCount(pv=pv_7, windowEnd=1511668800000, count=5886)
        //3> PvCount(pv=pv_5, windowEnd=1511668800000, count=5823)
        //3> PvCount(pv=pv_0, windowEnd=1511668800000, count=5917)
        //3> PvCount(pv=pv_1, windowEnd=1511668800000, count=5802)
        //3> PvCount(pv=pv_0, windowEnd=1511672400000, count=5620)
        //3> PvCount(pv=pv_7, windowEnd=1511672400000, count=5616)
        //3> PvCount(pv=pv_1, windowEnd=1511672400000, count=5432)
        //3> PvCount(pv=pv_5, windowEnd=1511672400000, count=5527)
        //3> PvCount(pv=pv_5, windowEnd=1511676000000, count=6183)
        //3> PvCount(pv=pv_0, windowEnd=1511676000000, count=6090)
        //3> PvCount(pv=pv_7, windowEnd=1511676000000, count=5990)
        //3> PvCount(pv=pv_1, windowEnd=1511676000000, count=6080)
        //3> PvCount(pv=pv_1, windowEnd=1511679600000, count=6464)
        //3> PvCount(pv=pv_7, windowEnd=1511679600000, count=6368)
        //3> PvCount(pv=pv_0, windowEnd=1511679600000, count=6323)
        //3> PvCount(pv=pv_5, windowEnd=1511679600000, count=6344)
        //3> PvCount(pv=pv_0, windowEnd=1511683200000, count=6496)
        //3> PvCount(pv=pv_5, windowEnd=1511683200000, count=6429)
        //3> PvCount(pv=pv_1, windowEnd=1511683200000, count=6567)
        //3> PvCount(pv=pv_7, windowEnd=1511683200000, count=6419)
        //3> PvCount(pv=pv_0, windowEnd=1511686800000, count=6514)
        //3> PvCount(pv=pv_1, windowEnd=1511686800000, count=6597)
        //3> PvCount(pv=pv_7, windowEnd=1511686800000, count=6554)
        //3> PvCount(pv=pv_5, windowEnd=1511686800000, count=6710)
        //3> PvCount(pv=pv_7, windowEnd=1511690400000, count=5931)
        //3> PvCount(pv=pv_0, windowEnd=1511690400000, count=6010)
        //3> PvCount(pv=pv_1, windowEnd=1511690400000, count=6092)
        //3> PvCount(pv=pv_5, windowEnd=1511690400000, count=6057)
        //3> PvCount(pv=pv_1, windowEnd=1511694000000, count=2)
        //3> PvCount(pv=pv_7, windowEnd=1511694000000, count=2)
        //3> PvCount(pv=pv_5, windowEnd=1511694000000, count=3)


        //4 按照窗口结束时间重新分组
//        SingleOutputStreamOperator<PvCount> count = aggregate.keyBy(data -> data.getWindowEnd()) //每个窗口结尾时间及对应的8个分组
        //而我需要的是，一个时间窗口，一个结果。
//                .sum("count"); //0-8每个组，其中每个组的每一个窗口
        SingleOutputStreamOperator<String> result = aggregate.keyBy(data -> data.getWindowEnd())
                .process(new PvCountProcessFunc());

        //5.打印输出
        result.print();
        //6.执行
        env.execute();


    }

    //<IN, ACC, OUT>
    public static class PvCountAggFunc implements AggregateFunction<Tuple2<String, Integer>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Integer> value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    //<IN, OUT, KEY, W extends Window>
    private static class PvCountWindowFunc implements WindowFunction<Long, PvCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<PvCount> out) throws Exception {
            String field = tuple.getField(0);
            out.collect(new PvCount(field, window.getEnd(), input.iterator().next()));
        }
    }

    //<K, I, O>
    public static class PvCountProcessFunc extends KeyedProcessFunction<Long, PvCount, String> {

        //定义集合状态
        private ListState<PvCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<PvCount>("list-state", PvCount.class));
        }

        @Override
        public void processElement(PvCount value, Context ctx, Collector<String> out) throws Exception {

            listState.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态信息
            Iterator<PvCount> iterator = listState.get().iterator();

            //定义最终一个小时的数据总和
            Long count = 0L;

            //遍历集合数据,累加结果
            while (iterator.hasNext()) {
                count += iterator.next().getCount();
            }

            //输出结果数据
            out.collect(ctx.getCurrentKey() +" PV:" + count);

            //清空状态
            listState.clear();
        }
    }
}
