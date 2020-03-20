package com.lotut.es_senior;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author shuai
 * @date 2019/10/12 13:15
 */
public class ScollDownloadSalesDataApp {
    public static void main(String[] args) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("car_shop").setTypes("cars")
                .setQuery(QueryBuilders.termQuery("brand.raw", "宝马"))
                .setScroll(new TimeValue(60000))
                .setSize(1)
                .get();

        int batchcount = 0;
        do {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println("batch: " + ++batchcount);
                System.out.println(hit.getSourceAsString());
                //每次查询一批数据 TODO
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
        } while (searchResponse.getHits().getHits().length != 0);
        client.close();
    }
}
