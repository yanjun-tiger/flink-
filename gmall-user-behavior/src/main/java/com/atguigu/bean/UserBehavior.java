package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 每隔5分钟输出最近一小时内点击量最多的前N个商品
 * @author zhouyanjun
 * @create 2020-11-25 19:17
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId; //用户id
    private Long itemId; // 商品id
    private Integer categoryId; // 商品所属类别
    private String behavior; // 用户行为类型 pv buy cart fav
    private Long timestamp; // 行为发生的时间戳
}
