package com.lotut.es_senior;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;

/**
 * @author shuai
 * @date 2019/10/11 10:12
 */
public class BulkUploadSalesDataApp {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("car_shop", "cars", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("brand", "奔驰")
                        .field("name", "奔驰C200")
                        .endObject()
                );
        bulkRequestBuilder.add(indexRequestBuilder);

        UpdateRequestBuilder updateRequestBuilder = client.prepareUpdate("car_shop", "cars", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("sale_price", 290000)
                        .endObject()
                );
        bulkRequestBuilder.add(updateRequestBuilder);

        DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete("car_shop", "cars", "2");
        bulkRequestBuilder.add(deleteRequestBuilder);

        BulkResponse bulkItemResponses = bulkRequestBuilder.get();

        for (BulkItemResponse bulkItemRespons : bulkItemResponses) {
            System.out.println(bulkItemRespons.getVersion());
        }
    }
}
