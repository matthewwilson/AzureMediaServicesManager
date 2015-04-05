package so.mwil.mediaservicesmanager;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

import java.net.URLEncoder;

public class Main {

    public static void main(String[] args) throws Exception {
        PropertiesHelper properties = new PropertiesHelper();

        String clientId = properties.getProperty("clientId");
        String clientSecret = properties.getProperty("clientSecret");
        String streamingEndpointName = properties.getProperty("streamingEndpointName");
        String channelName = properties.getProperty("channelName");
        String assetName = properties.getProperty("assetName");
        String programName = properties.getProperty("programName");
        String accessPolicyName = properties.getProperty("accessPolicyName");
        String locatorName = properties.getProperty("locatorName");


        String accessToken = HttpHelpers.getAccessToken(clientId, clientSecret);
        Header accessTokenHeader = new BasicHeader("Authorization","Bearer "+accessToken);

        String redirectUri = HttpHelpers.getRedirectUri(accessTokenHeader);

        startStreamingEndpoint(redirectUri, accessTokenHeader, streamingEndpointName);

        String channelId = startChannel(redirectUri, accessTokenHeader, channelName);

        String assetId = createAsset(redirectUri, accessTokenHeader, assetName);

        String programId = createProgram(redirectUri, accessTokenHeader, programName, channelId, assetId);

        startProgram(redirectUri, accessTokenHeader, programId);

        String accessPolicyId = createAccessPolicy(redirectUri, accessTokenHeader, accessPolicyName);

        JSONObject locator = createLocator(redirectUri, accessTokenHeader, locatorName, channelId, assetId, accessPolicyId);
        JSONObject asset = retrieveAsset(redirectUri, accessTokenHeader, assetId);
        JSONObject assetFiles = retrieveAssetFiles(redirectUri, accessTokenHeader, assetId);

        String baseUrl = locator.getString("Path");
        String manifestUrl = baseUrl;

        JSONArray assetFilesArray = assetFiles.getJSONArray("value");
        for(int i = 0; i < assetFilesArray.length(); i++) {
            JSONObject assetFile = (JSONObject) assetFilesArray.get(i);

            if(assetFile.getString("Name").endsWith(".ism")) {
                manifestUrl = manifestUrl + assetFile.getString("Name") + "/Manifest";
            }
        }

        System.out.println(manifestUrl);
    }

    private static JSONObject retrieveAssetFiles(String redirectUri, Header accessTokenHeader, String assetId) throws Exception {
        System.out.println("### BEGINNING RETRIEVE ASSET FILES PROCESS ###");

        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "Assets('"+assetId+"')/Files", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        System.out.println("### FINISHED RETRIEVE ASSET FILES PROCESS ###");

        return getJsonResponse;
    }

    private static JSONObject retrieveAsset(String redirectUri, Header accessTokenHeader, String assetId) throws Exception {
        System.out.println("### BEGINNING RETRIEVE ASSET PROCESS ###");

        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "Assets('"+assetId+"')", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        System.out.println("### FINISHED RETRIEVE ASSET PROCESS ###");

        return getJsonResponse;
    }

    private static String createAccessPolicy(String redirectUri, Header accessTokenHeader, String accessPolicyName) throws Exception {

        System.out.println("### BEGINNING CREATE ACCESS POLICY PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "AccessPolicies", accessTokenHeader);

        JSONObject body = new JSONObject();
        body.put("Name",accessPolicyName);
        body.put("DurationInMinutes", 120);
        body.put("Permissions",1); //read

        StringEntity input = new StringEntity(body.toString());
        input.setContentType("application/json;odata=minimalmetadata");
        post.setEntity(input);

        JSONObject postJsonResponse = HttpHelpers.executeAndGetJsonResponse(post, 201);

        System.out.println("### FINISHED CREATE ACCESS POLICY PROCESS ###");

        return postJsonResponse.getString("Id");

    }

    private static JSONObject createLocator(String redirectUri, Header accessTokenHeader, String locatorName, String channelId, String assetId, String accessPolicyId) throws Exception {
        System.out.println("### BEGINNING CREATE LOCATOR PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "Locators", accessTokenHeader);

        JSONObject body = new JSONObject();
        body.put("Name",locatorName);
        body.put("Type", 2);
        body.put("AssetId",assetId);
        body.put("AccessPolicyId",accessPolicyId);

        StringEntity input = new StringEntity(body.toString());
        input.setContentType("application/json;odata=minimalmetadata");
        post.setEntity(input);

        JSONObject postJsonResponse = HttpHelpers.executeAndGetJsonResponse(post, 201);

        System.out.println("### FINISHED CREATE LOCATOR PROCESS ###");

        return postJsonResponse;
    }

    private static void startProgram(String redirectUri, Header accessTokenHeader, String programId) throws Exception {
        System.out.println("### BEGINNING START PROGRAM PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "Programs('"+programId+"')/Start", accessTokenHeader);

        String postResponseOperationId = HttpHelpers.executeAndGetOperationId(post, 202);

        System.out.println("Program has been started, waiting for state to change to Succeeded");
        if(waitForOperationToComplete(redirectUri, postResponseOperationId, accessTokenHeader, 100)) {
            System.out.println("Successfully started Program");
        }

        System.out.println("### FINISHED START PROGRAM PROCESS ###");
    }

    private static String createAsset(String redirectUri, Header accessTokenHeader, String assetName) throws Exception {
        System.out.println("### BEGINNING CREATE ASSET PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "Assets", accessTokenHeader);

        JSONObject body = new JSONObject();
        body.put("Name",assetName);

        StringEntity input = new StringEntity(body.toString());
        input.setContentType("application/json;odata=minimalmetadata");
        post.setEntity(input);

        JSONObject postJsonResponse = HttpHelpers.executeAndGetJsonResponse(post, 201);

        System.out.println("### FINISHED CREATE ASSET PROCESS ###");

        return postJsonResponse.getString("Id");
    }

    private static String createProgram(String redirectUri, Header accessTokenHeader, String programName, String channelId, String assetId) throws Exception {
        System.out.println("### BEGINNING CREATE PROGRAM PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "Programs", accessTokenHeader);

        JSONObject body = new JSONObject();
        body.put("Name",programName);
        body.put("Description","A program for sunday morning services");
        body.put("ChannelId",channelId);
        body.put("AssetId",assetId);
        body.put("ArchiveWindowLength","PT1H");

        StringEntity input = new StringEntity(body.toString());
        input.setContentType("application/json;odata=minimalmetadata");
        post.setEntity(input);

        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(post, 201);

        String programId = getJsonResponse.getString("Id");

        System.out.println("### FINISHED CREATE PROGRAM PROCESS ###");

        return programId;
    }

    private static String startChannel(String redirectUri, Header accessTokenHeader, String channelName) throws Exception {
        System.out.println("### BEGINNING START CHANNEL PROCESS ###");

        System.out.println("Fetching available channels");
        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "Channels", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        JSONArray channels = getJsonResponse.getJSONArray("value");

        JSONObject channelToStart = null;

        for(int i = 0; i < channels.length(); i++) {

            JSONObject channel = (JSONObject) channels.get(i);

            if(channel.getString("Name").equalsIgnoreCase(channelName)) {
                System.out.println("Found the "+channelName+" channel");
                channelToStart = channel;
                break;
            }
        }

        if(channelToStart == null) {
            throw new Exception("Unable to find channel with name: "+channelName);
        }

        if(channelToStart.getString("State").equals("Stopped")) {

            System.out.println("Channel is in the Stopped state, trying to start it up");

            String encodedID = URLEncoder.encode(channelToStart.getString("Id"), "UTF-8");
            HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "Channels('" + encodedID + "')/Start", accessTokenHeader);
            String postResponseOperationId = HttpHelpers.executeAndGetOperationId(post, 202);

            System.out.println("Channel has been started, waiting for state to change to Succeeded");
            if(waitForOperationToComplete(redirectUri, postResponseOperationId, accessTokenHeader, 100)) {
                System.out.println("Successfully started Channel");
            }

        } else if(!channelToStart.getString("State").equals("Running")) {
            throw new Exception("Endpoint was in an undesirable state: "+channelToStart.getString("State"));
        }

        System.out.println("### FINISHED START CHANNEL PROCESS ###");
        return channelToStart.getString("Id");
    }

    private static void startStreamingEndpoint(String redirectUri, Header accessTokenHeader, String endpointName) throws Exception {

        System.out.println("### BEGINNING START STREAMING ENDPOINT PROCESS ###");

        System.out.println("Fetching available streaming endpoints");
        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "StreamingEndpoints", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        JSONArray streamingEndpoints = getJsonResponse.getJSONArray("value");

        JSONObject streamingEndpointToStart = null;

        for(int i = 0; i < streamingEndpoints.length(); i++) {

            JSONObject streamingEndpoint = (JSONObject) streamingEndpoints.get(i);

            if(streamingEndpoint.getString("Name").equalsIgnoreCase(endpointName)) {
                System.out.println("Found the "+streamingEndpoint+" streaming endpoint");
                streamingEndpointToStart = streamingEndpoint;
                break;
            }
        }

        if(streamingEndpointToStart == null) {
            throw new Exception("Unable to find endpoint with name: "+endpointName);
        }

        if(streamingEndpointToStart.getString("State").equals("Stopped")) {

            System.out.println("Streaming Endpoint is in the Stopped state, trying to start it up");

            String encodedID = URLEncoder.encode(streamingEndpointToStart.getString("Id"), "UTF-8");
            HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "StreamingEndpoints('" + encodedID + "')/Start", accessTokenHeader);
            String postResponseOperationId = HttpHelpers.executeAndGetOperationId(post, 202);

            System.out.println("Streaming Endpoint has been started, waiting for state to change to Succeeded");
            if(waitForOperationToComplete(redirectUri, postResponseOperationId, accessTokenHeader, 100)) {
                System.out.println("Successfully started StreamingEndpoint");
            }

        } else if(!streamingEndpointToStart.getString("State").equals("Running")) {
            throw new Exception("Endpoint was in an undesirable state: "+streamingEndpointToStart.getString("State"));
        }

        System.out.println("### FINISHED START STREAMING ENDPOINT PROCESS ###");
    }

    private static boolean waitForOperationToComplete(String redirectUrl, String operationId, Header accessTokenHeader, int retryCount) throws Exception {

        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUrl+"Operations('"+operationId+"')", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        System.out.println("Checking state...");
        if(!getJsonResponse.getString("State").equals("Succeeded")) {

            System.out.println("State was "+getJsonResponse.getString("State")+" going to wait and try again in 10sec, there are "+retryCount+" attempts left.");

            if(retryCount == 0) {
                throw new Exception("Timed out waiting for operation to be in the Succeeded state, actual state was: "+getJsonResponse.getString("State"));
            }

            Thread.sleep(10000);
            return waitForOperationToComplete(redirectUrl, operationId, accessTokenHeader, retryCount-1);
        }

        return getJsonResponse.getString("State").equals("Succeeded");
    }


}
