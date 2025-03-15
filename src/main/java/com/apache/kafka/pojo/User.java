package com.apache.kafka.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private int Id;
    private String name;
    private String email;
    private String phone;
    private String ipAddress;
}
