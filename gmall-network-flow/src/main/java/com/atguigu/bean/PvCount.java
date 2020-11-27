package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PvCount {

    private String pv; // pv字段
    private Long windowEnd; //窗口结尾字段
    private Long count;  //计数

}
