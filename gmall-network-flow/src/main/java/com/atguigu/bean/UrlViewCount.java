package com.atguigu.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UrlViewCount {
    private String url; //网页链接
    private Long windowEnd; // 窗口结尾时间戳
    private Long count; // 计数
}
