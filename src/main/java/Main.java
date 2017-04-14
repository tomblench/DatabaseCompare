import com.cloudant.client.api.ClientBuilder;
import com.cloudant.client.api.CloudantClient;
import com.cloudant.client.api.Database;
import com.cloudant.client.api.model.Params;
import com.cloudant.http.Http;
import com.cloudant.http.HttpConnection;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Created by tomblench on 13/04/2017.
 */
public class Main {


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

    public static void compare(URL databaseUrl1, String databaseName1, URL databaseUrl2, String databaseName2) throws IOException {

        CloudantClient client1 = ClientBuilder.url(databaseUrl1).build();
        CloudantClient client2 = ClientBuilder.url(databaseUrl2).build();

        Database database1 = client1.database(databaseName1, false);
        Database database2 = client2.database(databaseName2, false);

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

        Map<String, List<String>> missingRevsInDb2 = getMissingRevs(common, database1, client2, databaseName2);
        Map<String, List<String>> missingRevsInDb1 = getMissingRevs(common, database2, client1, databaseName1);

        System.out.println("Missing revs in db 1:"+missingRevsInDb1);
        System.out.println("Missing revs in db 2:"+missingRevsInDb2);
    }

    public static Map<String, List<String>>  getMissingRevs(Set<String> docIds, Database database1, CloudantClient client2, String databaseName2) {

        Map<String, List<String>> revsToRequest = new HashMap<>();

        for (String docId : docIds) {
            // get winning
            Map<String, Object> h2 = database1.find(HashMap.class, docId);
            String rev = (String)h2.get("_rev");
            List<String> revisions = new ArrayList<String>();
            revisions.add(rev);
            // get others
            Params p = new Params();
            p.addParam("conflicts", "true");
            Map<String, Object> h = database1.find(HashMap.class, docId, p);
            List<String> others = (List<String>)(h.get("_conflicts"));
            if (others != null) {
                revisions.addAll(others);
            }
            revsToRequest.put(docId, revisions);
        }
        // TODO fix terrible URL mangling
        try {
            String jsonRequest = new GsonBuilder().create().toJson(revsToRequest);
            System.out.println("request "+jsonRequest);
            HttpConnection hc = Http.POST(new URL(client2.getBaseUri() + "/" + databaseName2 + "/_missing_revs"),
                    "application/json");
            hc.setRequestBody(jsonRequest);
            hc = client2.executeRequest(hc);
            String response = hc.responseAsString();
            Map responseMap = new GsonBuilder().create().fromJson(response, Map.class);
            Map<String,List<String>> responseMapMissingRevs = (Map<String,List<String>>)responseMap.get("missing_revs");
            return responseMapMissingRevs;
        } catch (MalformedURLException mue ) {
            // TODO
            System.err.println(mue);
        } catch (IOException ioe) {
            // TODO
            System.err.println(ioe);
        }
        return null;
    }


}
