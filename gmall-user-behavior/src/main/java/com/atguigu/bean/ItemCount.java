package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品
 * @author zhouyanjun
 * @create 2020-11-25 19:24
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount {
    private Long itemId; // 商品id
    private Long windowEnd;// 窗口的结束时间
    private Long count;// 点击次数
}
