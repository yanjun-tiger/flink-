package com.atguigu.app;

import com.atguigu.bean.ItemCount;
import com.atguigu.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 需求：每隔5分钟输出最近一小时内点击量最多的前N个商品
 *
 * @author zhouyanjun
 * @create 2020-11-25 19:29
 */
public class HotItemApp {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从文件读取数据创建流并转换为JavaBean同时提取事件时间
        SingleOutputStreamOperator<UserBehavior> userDS = env.readTextFile("D:\\workspace_idea1\\gmall-flink-200621\\input\\UserBehavior.csv")
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
                        return element.getTimestamp()*1000L; //数据中是秒，flink里都是毫秒
                    }
                });


        //3.按照"pv"过滤,按照itemID分组,聚合,计算数据
        SingleOutputStreamOperator<ItemCount> itemCountDS = userDS.filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(data -> data.getItemId())//这样写，不改变类型。如果keyBy("itemId")那类型就是tuple，窗口函数作为泛型时，会报错。方便起见，这样写
                .timeWindow(Time.hours(1), Time.minutes(5))
                //AggregateFunction<T, ACC, V> aggFunction,WindowFunction<V, R, K, W> windowFunction
                .aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        //4.按照windowEnd分组,需要统计出每个窗口的结果，然后根据结果进行排序输出
        SingleOutputStreamOperator<String> result = itemCountDS.keyBy("windowEnd")
                .process(new TopNItemIdCountProcessFunc(5)); //流式数据，来一条处理一条。这里需要保存状态，然后根据状态进行排序

        //5.打印结果
        result.print();

        //6.执行任务
        env.execute();
    }

    //自定义增量聚合函数
    //<IN, ACC, OUT> (input values) (intermediate aggregate state) aggregated result
    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        //定义一个聚合状态的初始值
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        //每来一个元素，聚合状态发生的变化
        //这里的Long是要改变成的类型
        public Long add(UserBehavior value, Long accumulator) {
            //每来一个value，聚合状态+1
            return accumulator + 1L;
        }

        @Override
        //输出
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        //一般只有session操作，才会用merge
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


    //自定义window函数
    //<IN, OUT, KEY, W extends Window>  input value. output value. type of the key. The type of {@code Window} that this window function can be applied on
    public static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount, Long, TimeWindow> {
        @Override
        //做计算前，把窗口当中的所有数据放到一个迭代器里面，然后把迭代器传过来，根据迭代器做相应计算
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            long windowEnd = window.getEnd();//获取此窗口的结束时间戳
            //因为ItemCountAggFunc传递过来的是1个聚合值，所以直接用.next()进行输出值
            Long count = input.iterator().next(); //Iterator对象称为迭代器(设计模式的一种)，主要用于遍历 Collection 集合中的元素。
            out.collect(new ItemCount(itemId, windowEnd, count));
        }
    }


    //自定排序KeyedProcessFunction
    //<K, I, O> key input /output elements.
    public static class TopNItemIdCountProcessFunc extends KeyedProcessFunction<Tuple, ItemCount, String> {
        //TopN属性
        private Integer topSize;

        //空参构造器
        public TopNItemIdCountProcessFunc() {
        }

        //带参构造器
        public TopNItemIdCountProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        //定义ListState同于存放相同Key[windowEnd]的数据
        //把每条结果保存到状态中。因为这是流的处理，不保存状态，处理完就结束了
        private ListState<ItemCount> listState;

        //生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            //获得状态的句柄，句柄是什么意思？
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("list-state", ItemCount.class));
        }

        @Override
        //处理每一条数据
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据,将数据存入集合  状态
            listState.add(value);
            //注册定时器  需要Context ctx。 窗口的所有数据收集完毕之后，触发定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        //定时器时间到了，触发计算  对窗口内的所有数据进行排序
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的所有数据
            Iterator<ItemCount> iterator = listState.get().iterator();//ListState.get()返回Iterable<T>
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);//newArrayList可变数组

            //排序
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if (o1.getCount() > o2.getCount()) {
                        return -1;
                    } else if (o1.getCount() < o2.getCount()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            //StringBuilder:可变的字符序列；jdk5.0声明的，线程不安全的，效率高；底层使用char[]存储
            StringBuilder sb = new StringBuilder();
            sb.append("======================\n");
            sb.append("当前窗口结束时间为:").append(new Timestamp(timestamp - 1L)).append("\n");

            //取前topSize条数据输出  下面这样写，是为了避免空指针的问题
            for (int i = 0; i < Math.min(topSize, itemCounts.size()); i++) {
                //取出数据
                ItemCount itemCount = itemCounts.get(i); //list的查操作：get(int index)
                sb.append("TOP ").append(i + 1);// 从0开始的
                sb.append(" ItemId=").append(itemCount.getItemId()); //商品id
                sb.append(" 商品热度=").append(itemCount.getCount()); //商品点击次数
                sb.append("\n");
            }
            sb.append("======================\n\n");
            //排序结束之后，可以清空状态 不然一直堆积在内存中，会把内存搞爆
            listState.clear();
            Thread.sleep(1000);

            //输出数据
            out.collect(sb.toString());
        }
    }
}
