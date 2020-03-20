package com.lotut.test.com.lotut.se_senior;

import com.lotut.domain.CarInfo;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * @author shuai
 * @date 2019/10/11 10:51
 */
public class CarInfoApp {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

    }

    /**
     * upsert数据
     *
     * @param client
     * @param carInfo
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void upsertCarInfo(TransportClient client, CarInfo carInfo) throws IOException, ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest("car_shop", "cars", carInfo.getId())
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("brand", carInfo.getBrand())
                        .field("name", carInfo.getName())
                        .field("price", carInfo.getPrice())
                        .field("produce_date", carInfo.getProoduceDate())
                        .field("sale_price", carInfo.getSalePrice())
                        .field("sale_date", carInfo.getSaleDate())
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest("car_shop", "cars", carInfo.getId())
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("brand", carInfo.getBrand())
                        .field("name", carInfo.getName())
                        .field("price", carInfo.getPrice())
                        .field("produce_date", carInfo.getProoduceDate())
                        .field("sale_price", carInfo.getSalePrice())
                        .field("sale_date", carInfo.getSaleDate())
                        .endObject())
                .upsert(indexRequest);
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println(updateResponse.getVersion() + "  " + updateResponse.getResult().getLowercase());
    }

    /**
     * delete 数据
     *
     * @param client
     * @param id
     * @return
     * @throws UnknownHostException
     */
    public static DeleteRequestBuilder getDeleteRequestBuilder(TransportClient client, String id) throws UnknownHostException {
        return client.prepareDelete("car_shop", "cars", id);
    }

    /**
     * 根据id批量查询数据
     *
     * @param client
     * @param ids
     * @throws UnknownHostException
     */
    public static void mGetCarInfo(TransportClient client, String... ids) throws UnknownHostException {
        if (ids.length > 0) {
            MultiGetRequestBuilder multiGetRequestBuilder = client.prepareMultiGet().add("car_shop", "cars", ids);
            MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.get();
            for (MultiGetItemResponse multiGetItemRespons : multiGetItemResponses) {
                System.out.println(multiGetItemRespons.getResponse().getSourceAsString());
            }
        }
    }

    /**
     * 获取indexRequest
     *
     * @param client
     * @param carInfo
     * @return
     * @throws IOException
     */
    public static IndexRequestBuilder getIndexRequestBuilder(TransportClient client, CarInfo carInfo) throws IOException {
        return client.prepareIndex("car_shop", "cars", carInfo.getId())
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("brand", carInfo.getBrand())
                        .field("name", carInfo.getName())
                        .field("price", carInfo.getPrice())
                        .field("produce_date", carInfo.getProoduceDate())
                        .field("sale_price", carInfo.getSalePrice())
                        .field("sale_date", carInfo.getSaleDate())
                        .endObject());
    }

    /**
     * 统一执行增删改操作
     *
     * @param client
     * @param requestBuilders
     * @return
     */
    public static BulkResponse bulkResponse(TransportClient client, ReplicationRequestBuilder... requestBuilders) {
        if (requestBuilders.length > 0) {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            for (ReplicationRequestBuilder requestBuilder : requestBuilders) {
                if (requestBuilder instanceof DeleteRequestBuilder) {
                    bulkRequestBuilder.add((DeleteRequestBuilder) requestBuilder);
                }
                if (requestBuilder instanceof IndexRequestBuilder) {
                    bulkRequestBuilder.add((IndexRequestBuilder) requestBuilder);
                }
            }
            return bulkRequestBuilder.get();
        }
        return null;
    }

    /**
     * 批量滚动scroll查询所有数据
     *
     * @param client
     */
    public static void scrollAllDate(TransportClient client) {
        SearchResponse searchResponse = client.prepareSearch()
                .setIndices("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(1)
                .setScroll(new TimeValue(60000))
                .get();
        int matchCount = 0;

        while (searchResponse.getHits().getHits().length != 0) {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println(hit.getId());
                System.out.println(hit.getSourceAsString());
                System.out.println("matchCount：" + ++matchCount);
                //各种操作 TODO
            }
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
        }
    }

    /**
     * 获取ES client对象
     *
     * @return
     * @throws UnknownHostException
     */
    private static TransportClient getTransportClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        return new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }

}
