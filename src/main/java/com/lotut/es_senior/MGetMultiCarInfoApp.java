package com.lotut.es_senior;

import org.elasticsearch.action.get.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author shuai
 * @date 2019/10/11 9:52
 */
public class MGetMultiCarInfoApp {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
                .add("car_shop", "cars", new String[]{"1", "2"})
                .get();

        for (MultiGetItemResponse multiGetItemRespons : multiGetItemResponses) {
            GetResponse response = multiGetItemRespons.getResponse();
            if (response.isExists()) {
                System.out.println(response.getSourceAsString());
            }
        }
        client.close();
    }
}
