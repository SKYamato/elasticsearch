package com.lotut.es_senior;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author shuai
 * @date 2019/10/12 15:18
 */
public class GeoLocationShopSearchApp {
    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

        SearchResponse searchResponse = client.prepareSearch("car_shop")
                .setTypes("shops")
                //矩形范围搜索
                .setQuery(QueryBuilders.geoBoundingBoxQuery("pin.location")
                        .setCorners(40.73, -74.1, 40.01, -71.12))
                .get();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        System.out.println("——————————————————");

        List<GeoPoint> points = new ArrayList<GeoPoint>();
        points.add(new GeoPoint(40.73, -74.1));
        points.add(new GeoPoint(40.01, -71.12));
        points.add(new GeoPoint(50.56, -90.58));

        searchResponse = client.prepareSearch("car_shop")
                .setTypes("shops")
                //三个点构成的区域内搜索
                .setQuery(QueryBuilders.geoPolygonQuery("pin.location", points))
                .get();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }


        System.out.println("===========================================");

        searchResponse = client.prepareSearch()
                .setIndices("car_shop")
                .setTypes("shops")
                //距离一个点指定半径范围搜索
                .setQuery(QueryBuilders.geoDistanceQuery("pin.location")
                        .point(40, -70)
                        .distance(200, DistanceUnit.KILOMETERS))
                .get();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsString());
        }

        client.close();
    }
}
