package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
//每隔5秒，输出最近10分钟内访问量最多的前N个URL。
public class ApacheLog {

    private String ip;  //访问的ip
    private String userId;// 访问的userId
    private Long eventTime;// 访问时间
    private String method;// 访问方法 get post put delete
    private String url;// 访问的url

}
