package com.lotut.es_senior;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author shuai
 * @date 2019/10/12 14:44
 */
public class FullTextCarInfo {
    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        SearchResponse searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.matchQuery("brand", "华晨宝马"))
                .get();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        System.out.println("——————————————————————");

        searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.multiMatchQuery("宝马", "brand", "name"))
                .get();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("——————————————————————");

        searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.termQuery("name.raw", "宝马318"))
                .get();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        System.out.println("——————————————————————");

        searchResponse = client.prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.prefixQuery("name.raw", "宝"))
                .get();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }
        client.close();
    }
}
