package com.lotut.es_senior;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * upsert汽车信息
 *
 * @author shuai
 * @date 2019/10/10 17:38
 */
public class UpsertCarInfoApp {
    public static void main(String[] args) throws Exception {
        //upsertBMWInfo();
        upsertBenzInfo();
    }

    private static void upsertBMWInfo() throws Exception {
        //设置Client
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();
        //创建Client
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        //IndexRequest
        IndexRequest indexRequest = new IndexRequest("car_shop", "cars", "1")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("brand", "宝马")
                        .field("name", "宝马320")
                        .field("price", 320000)
                        .field("produce_date", "2017-01-01")
                        .endObject()
                );
        //UpdateRequest
        UpdateRequest updateRequest = new UpdateRequest("car_shop", "cars", "1")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("price", 310000)
                        .endObject())
                .upsert(indexRequest);
        //执行upsert操作
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println(updateResponse.getVersion());
        System.out.println(updateResponse.getResult().getLowercase());
        client.close();
    }

    private static void upsertBenzInfo() throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        IndexRequest indexRequest = new IndexRequest("car_shop", "cars", "2")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("brand", "奔驰")
                        .field("name", "奔驰C200")
                        .field("price", 350000)
                        .field("produce_date", "2017-01-05")
                        .endObject()
                );
        UpdateRequest updateRequest = new UpdateRequest("car_shop", "cars", "2")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("brand", "奔驰")
                        .field("name", "奔驰C200")
                        .field("price", 350000)
                        .field("produce_date", "2017-01-05")
                        .endObject()
                ).upsert(indexRequest);
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println(updateResponse.getResult().getLowercase());
        client.close();
    }
}
