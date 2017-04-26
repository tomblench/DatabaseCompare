import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.http.Http;
import com.cloudant.http.HttpConnection;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.net.URL;
import java.util.*;

/**
 * Created by tomblench on 13/04/2017.
 */
public class Main {

    private static String fakeRevisionId = "9999-a";
    private static Gson gson = new GsonBuilder().create();

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Error: Usage: DatabaseCompare url1 database1 url2 database2");
        }
        try {
            compare(new URL(args[0]), args[1],
                new URL(args[2]), args[3]);
        } catch (Exception e) {
            System.err.println("Error: "+e);
            e.printStackTrace();
        }
    }

    public static void compare(URL databaseUrl1, String databaseName1, URL databaseUrl2, String databaseName2) throws Exception {

        CloudantClient client1 = ClientBuilder.url(databaseUrl1).build();
        CloudantClient client2 = ClientBuilder.url(databaseUrl2).build();

        Database database1 = client1.database(databaseName1, false);
        Database database2 = client2.database(databaseName2, false);

        System.out.println("Getting all document ids");

        List<String> allDocs1 = database1.getAllDocsRequestBuilder().build().getResponse().getDocIds();
        List<String> allDocs2 = database2.getAllDocsRequestBuilder().build().getResponse().getDocIds();

        Set<String> onlyInDb1 = new HashSet<>(allDocs1);
        onlyInDb1.removeAll(allDocs2);

        Set<String> onlyInDb2 = new HashSet<>(allDocs2);
        onlyInDb2.removeAll(allDocs1);

        System.out.println("Documents only in db 1:"+ onlyInDb1);
        System.out.println("Documents only in db 2:"+ onlyInDb2);

        Set<String> common = new HashSet<>(allDocs1);
        common.retainAll(allDocs2);
        List<List<String>> batches = partition(common, 500);

        Map<String, List<String>> missingRevsInDb2 = getMissingRevs(batches, client1, databaseName1, client2, databaseName2);
        Map<String, List<String>> missingRevsInDb1 = getMissingRevs(batches, client2, databaseName2, client1, databaseName1);

        System.out.println("Missing revs in db 1:"+missingRevsInDb1);
        System.out.println("Missing revs in db 2:"+missingRevsInDb2);
    }

    public static <T> List<List<T>> partition(Collection list, int size) {
        int i = 0;
        List<List<T>> output = new ArrayList<>();
        List current = null;
        for (Object item : list) {
            if (i++ % size == 0) {
                current = new ArrayList();
                output.add(current);
            }
            current.add(item);
        }
        return output;
    }

    public static Map<String, List<String>> getMissingRevs(List<List<String>> batches, CloudantClient client1, String databaseName1, CloudantClient client2, String databaseName2) throws Exception {

        Map<String, List<String>> missing = new HashMap<>();

        Map<String, List<String>> revsDiffRequestDb1 = new HashMap<>();
        Map<String, List<String>> revsDiffRequestDb2 = new HashMap<>();

        for (List<String> docIds : batches) {

            System.out.print(".");

            // look in db1 - use a fake revision ID to fetch all leaf revisions
            for (String docId : docIds) {
                revsDiffRequestDb1.put(docId, Collections.singletonList(fakeRevisionId));
            }
            String jsonRequestDb1 = gson.toJson(revsDiffRequestDb1);
            HttpConnection hc1 = Http.POST(new URL(client1.getBaseUri() + "/" + databaseName1 + "/_revs_diff"),
                    "application/json");
            hc1.setRequestBody(jsonRequestDb1);
            String response1 = client1.executeRequest(hc1).responseAsString();
            Map<String, Object> responseMap1 = (Map<String, Object>) gson.fromJson(response1, Map.class);
            for (String docId : responseMap1.keySet()) {
                Map<String, List<String>> entry = (Map<String, List<String>>) responseMap1.get(docId);
                List<String> revIds = entry.get("possible_ancestors");
                if (revIds != null) {
                    revsDiffRequestDb2.put(docId, revIds);
                }
            }

            // look in db2
            String jsonRequestDb2 = gson.toJson(revsDiffRequestDb2);
            HttpConnection hc2 = Http.POST(new URL(client2.getBaseUri() + "/" + databaseName2 + "/_revs_diff"),
                    "application/json");
            hc2.setRequestBody(jsonRequestDb2);
            String response2 = client2.executeRequest(hc2).responseAsString();
            Map<String, Object> responseMap2 = (Map<String, Object>) gson.fromJson(response2, Map.class);
            for (String docId : responseMap2.keySet()) {
                Map<String, List<String>> entry = (Map<String, List<String>>) responseMap2.get(docId);
                List<String> revIds = entry.get("missing");
                if (revIds != null) {
                    missing.put(docId, revIds);
                }
            }
        }
        System.out.println("");
        return missing;
    }

}
