package so.mwil.mediaservicesmanager;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpDelete;
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

        startStreaming(properties);
        stopStreaming(properties);
    }

    private static void stopStreaming(PropertiesHelper properties) throws Exception {

        String clientId = properties.getProperty("clientId");
        String clientSecret = properties.getProperty("clientSecret");
        String streamingEndpointName = properties.getProperty("streamingEndpointName");
        String channelName = properties.getProperty("channelName");
        String assetName = properties.getProperty("assetName");
        String programName = properties.getProperty("programName");

        String accessToken = HttpHelpers.getAccessToken(clientId, clientSecret);
        Header accessTokenHeader = new BasicHeader("Authorization","Bearer "+accessToken);

        String redirectUri = HttpHelpers.getRedirectUri(accessTokenHeader);

        JSONObject program = retrieveProgram(redirectUri, accessTokenHeader, programName);
        stopProgram(redirectUri, accessTokenHeader, program.getString("Id"));
        deleteProgram(redirectUri, accessTokenHeader, program.getString("Id"));

        String assetId = retrieveAssetId(redirectUri, accessTokenHeader, assetName);
        deleteAsset(redirectUri, accessTokenHeader, assetId);

        //stop channel
        String channelId = retrieveChannelId(redirectUri, accessTokenHeader, channelName);
        stopChannel(redirectUri, accessTokenHeader, channelId);

        //stop streaming endpoint
        String streamingEndpointId = retrieveStreamingEndpointId(redirectUri, accessTokenHeader, streamingEndpointName);
        stopStreamingEndpoint(redirectUri, accessTokenHeader, streamingEndpointId);
    }

    private static void stopStreamingEndpoint(String redirectUri, Header accessTokenHeader, String channelId) throws Exception {
        System.out.println("### BEGINNING STOP STREAMING ENDPOINT PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "StreamingEndpoints('"+channelId+"')/Stop", accessTokenHeader);

        String postResponseOperationId = HttpHelpers.executeAndGetOperationId(post, 202);

        System.out.println("Streaming Endpoint has been stopped, waiting for state to change to Stopped");
        if(waitForOperationToComplete(redirectUri, postResponseOperationId, accessTokenHeader, 100)) {
            System.out.println("Successfully stopped Streaming Endpoint");
        }

        System.out.println("### FINISHED STOP STREAMING ENDPOINT PROCESS ###");
    }

    private static String retrieveStreamingEndpointId(String redirectUri, Header accessTokenHeader, String streamingEndpointName) throws Exception {
        System.out.println("### BEGINNING RETRIEVE STREAMING ENDPOINT ID PROCESS ###");

        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "StreamingEndpoints", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        JSONArray assets = getJsonResponse.getJSONArray("value");

        for(int i = 0; i < assets.length(); i++) {

            JSONObject asset = (JSONObject) assets.get(i);

            if(asset.getString("Name").equalsIgnoreCase(streamingEndpointName)) {
                System.out.println("Found the "+streamingEndpointName+" channel");
                System.out.println("### FINISHED RETRIEVE STREAMING ENDPOINT ID PROCESS ###");
                return asset.getString("Id");
            }
        }

        throw new Exception("Couldn't find the channel!");
    }

    private static void stopChannel(String redirectUri, Header accessTokenHeader, String channelId) throws Exception {
        System.out.println("### BEGINNING STOP CHANNEL PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "Channels('"+channelId+"')/Stop", accessTokenHeader);

        String postResponseOperationId = HttpHelpers.executeAndGetOperationId(post, 202);

        System.out.println("Channel has been stopped, waiting for state to change to Stopped");
        if(waitForOperationToComplete(redirectUri, postResponseOperationId, accessTokenHeader, 100)) {
            System.out.println("Successfully stopped Channel");
        }

        System.out.println("### FINISHED STOP CHANNEL PROCESS ###");
    }

    private static String retrieveChannelId(String redirectUri, Header accessTokenHeader, String channelName) throws Exception {
        System.out.println("### BEGINNING RETRIEVE CHANNEL ID PROCESS ###");

        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "Channels", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        JSONArray assets = getJsonResponse.getJSONArray("value");

        for(int i = 0; i < assets.length(); i++) {

            JSONObject asset = (JSONObject) assets.get(i);

            if(asset.getString("Name").equalsIgnoreCase(channelName)) {
                System.out.println("Found the "+channelName+" channel");
                System.out.println("### FINISHED RETRIEVE CHANNEL ID PROCESS ###");
                return asset.getString("Id");
            }
        }

        throw new Exception("Couldn't find the channel!");
    }

    private static void deleteAsset(String redirectUri, Header accessTokenHeader, String assetId) throws Exception {
        System.out.println("### BEGINNING DELETE ASSET PROCESS ###");

        HttpDelete delete = HttpHelpers.GetHttpDeleteForUrl(redirectUri + "Assets('" + assetId + "')", accessTokenHeader);

        HttpHelpers.execute(delete, 204);

        System.out.println("### FINISHED DELETE ASSET PROCESS ###");
    }

    private static String retrieveAssetId(String redirectUri, Header accessTokenHeader, String assetName) throws Exception {
        System.out.println("### BEGINNING RETRIEVE ASSET ID PROCESS ###");

        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "Assets", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        JSONArray assets = getJsonResponse.getJSONArray("value");

        for(int i = 0; i < assets.length(); i++) {

            JSONObject asset = (JSONObject) assets.get(i);

            if(asset.getString("Name").equalsIgnoreCase(assetName)) {
                System.out.println("Found the "+assetName+" asset");
                System.out.println("### FINISHED RETRIEVE ASSET ID PROCESS ###");
                return asset.getString("Id");
            }
        }

        throw new Exception("Couldn't find the asset!");
    }

    private static void startStreaming(PropertiesHelper properties) throws Exception {

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

    private static void deleteProgram(String redirectUri, Header accessTokenHeader, String programId) throws Exception {
        System.out.println("### BEGINNING DELETE PROGRAM PROCESS ###");

        HttpDelete delete = HttpHelpers.GetHttpDeleteForUrl(redirectUri + "Programs('" + programId + "')", accessTokenHeader);

        HttpHelpers.execute(delete, 204);

        System.out.println("### FINISHED DELETE PROGRAM PROCESS ###");
    }

    private static JSONObject retrieveProgram(String redirectUri, Header accessTokenHeader, String programName) throws Exception {
        System.out.println("### BEGINNING RETRIEVE PROGRAM PROCESS ###");

        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUri + "Programs", accessTokenHeader);
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);

        JSONArray programs = getJsonResponse.getJSONArray("value");

        for(int i = 0; i < programs.length(); i++) {

            JSONObject program = (JSONObject) programs.get(i);

            if(program.getString("Name").equalsIgnoreCase(programName)) {
                System.out.println("Found the "+programName+" program");
                System.out.println("### FINISHED RETRIEVE PROGRAM PROCESS ###");
                return program;
            }
        }

        throw new Exception("Couldn't find the program!");
    }

    private static void stopProgram(String redirectUri, Header accessTokenHeader, String programId) throws Exception {
        System.out.println("### BEGINNING STOP PROGRAM PROCESS ###");

        HttpPost post = HttpHelpers.GetHttpPostForUrl(redirectUri + "Programs('"+programId+"')/Stop", accessTokenHeader);

        String postResponseOperationId = HttpHelpers.executeAndGetOperationId(post, 202);

        System.out.println("Program has been stopped, waiting for state to change to Stopped");
        if(waitForOperationToComplete(redirectUri, postResponseOperationId, accessTokenHeader, 100)) {
            System.out.println("Successfully stopped Program");
        }

        System.out.println("### FINISHED STOP PROGRAM PROCESS ###");
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

        System.out.println("Checking state...");
        HttpGet get = HttpHelpers.GetHttpGetForUrl(redirectUrl+"Operations('"+operationId+"')", accessTokenHeader);
        System.out.println("Getting state response...");
        JSONObject getJsonResponse = HttpHelpers.executeAndGetJsonResponse(get, 200);
        System.out.println("Checking state response...");

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
