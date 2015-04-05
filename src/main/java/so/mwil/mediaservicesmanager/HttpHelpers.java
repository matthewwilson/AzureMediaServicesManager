package so.mwil.mediaservicesmanager;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.AbstractHttpMessage;
import org.codehaus.jettison.json.JSONObject;

import java.net.URLEncoder;

/**
 * Created by matthew on 04/04/15.
 */
public class HttpHelpers {

    private static HttpClient httpClient = HttpClientBuilder.create().build();

    public static HttpPost GetHttpPostForUrl(String url, Header accessTokenHeader) {
        HttpPost post = new HttpPost(url);
        AddRequiredHeaders(accessTokenHeader, post);
        return post;
    }

    public static HttpGet GetHttpGetForUrl(String url, Header accessTokenHeader) {
        HttpGet get = new HttpGet(url);
        AddRequiredHeaders(accessTokenHeader, get);
        return get;
    }

    private static void AddRequiredHeaders(Header accessTokenHeader, AbstractHttpMessage post) {
        post.addHeader(accessTokenHeader);
        post.addHeader("DataServiceVersion", "3.0;NetFx");
        post.addHeader("MaxDataServiceVersion", "3.0;NetFx");
        post.addHeader("x-ms-version", "2.9");
    }

    public static String executeAndGetOperationId(HttpUriRequest request, int expectedResponseCode) throws Exception {
        request.addHeader("Accept","application/json;odata=minimalmetadata");

        HttpResponse response = httpClient.execute(request);

        if(response.getStatusLine().getStatusCode() == expectedResponseCode) {
            return response.getFirstHeader("operation-id").getValue();
        } else {
            throw new Exception("Unexpected status code was: "+response.getStatusLine().getStatusCode()+" expected "+expectedResponseCode);
        }
    }

    public static JSONObject executeAndGetJsonResponse(HttpUriRequest request, int expectedResponseCode) throws Exception {
        request.addHeader("Accept","application/json;odata=minimalmetadata");

        HttpResponse response = httpClient.execute(request);

        if(response.getStatusLine().getStatusCode() == expectedResponseCode) {
            String responseString = IOUtils.toString(response.getEntity().getContent());
            JSONObject responseJson = new JSONObject(responseString);

            return responseJson;

        } else {
            throw new Exception("Unexpected status code was: "+response.getStatusLine().getStatusCode()+" expected "+expectedResponseCode);
        }
    }

    public static String getRedirectUri(Header accessTokenHeader) throws Exception {
        HttpPost get = new HttpPost("https://media.windows.net/");
        get.addHeader(accessTokenHeader);

        get.addHeader("DataServiceVersion", "3.0");
        get.addHeader("MaxDataServiceVersion", "3.0");
        get.addHeader("x-ms-version", "2.8");

        HttpResponse response = httpClient.execute(get);

        if(response.getStatusLine().getStatusCode() == 301) {
            String responseString = IOUtils.toString(response.getEntity().getContent());

            int startIndex = responseString.indexOf("<a") + 9;
            int endIndex = responseString.indexOf(">h") -1;

            return responseString.substring(startIndex,endIndex);

        } else {
            throw new Exception("Unable to get redirect URL, status code was: "+response.getStatusLine().getStatusCode());
        }
    }

    public static String getAccessToken(String clientId, String clientSecret) throws Exception {
        HttpPost postAccessTokenRequest = new HttpPost("https://wamsprodglobal001acs.accesscontrol.windows.net/v2/OAuth2-13");

        String encodedClientSecret = URLEncoder.encode(clientSecret, "UTF-8");

        StringEntity input = new StringEntity("grant_type=client_credentials&client_id="+clientId+"&client_secret="+encodedClientSecret+"&scope=urn%3aWindowsAzureMediaServices");
        input.setContentType("application/x-www-form-urlencoded");
        postAccessTokenRequest.setEntity(input);

        HttpResponse response = httpClient.execute(postAccessTokenRequest);

        if(response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException("Could not get access token, HTTP ERROR CODE: "+ response.getStatusLine().getStatusCode());
        }

        String responseString = IOUtils.toString(response.getEntity().getContent());

        JSONObject responseJson = new JSONObject(responseString);

        String accessToken = responseJson.get("access_token").toString();

        if(accessToken == null) {
            throw new Exception("AccessToken was null");
        }

        return accessToken;
    }
}
