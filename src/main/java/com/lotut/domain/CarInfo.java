package com.lotut.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @author shuai
 * @date 2019/10/12 11:08
 */
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class CarInfo {
    private String id;
    private String brand;
    private String name;
    private int price;
    private String prooduceDate;
    private int salePrice;
    private String saleDate;
}
