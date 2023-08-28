package org.feedData;


import javax.swing.*;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
//import net.minidev.json.JSONArray;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.http.*;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.HttpEntity;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;



public class Main {
public static Connection db;

//public static String serverurl="http://217.160.150.140:8080";
public static String serverurl="http://35.239.135.31:8080";
public static void main(String []args) {

    try {
        Class.forName("org.postgresql.Driver");
    } catch (Exception k) {
        JOptionPane.showMessageDialog(null, k);
    }


    try {
        db = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stori", "postgres", "pazupazu");
    } catch (SQLException e) {
        JOptionPane.showMessageDialog(null, "Database Failure\n " + e);
    }

    while(true)
    {
        sendData();
        try {
            Thread.sleep(30000);
        }catch (Exception e)
        {

        }
    }

}

public static void sendData()
{




        JSONArray jsonArray = new JSONArray();
        try
        {
            String msq="SELECT num AS num, fname AS membername, id AS memberid, phone AS phone, trees AS trees, altphone AS altphone, (select fname from banks where num=members.bank) AS Bank, b_account AS baccount, (select sum(wei) from hawatrans where member=members.number and session='"+getSession()+"') AS cherrykgs, (select sum(wei) from mbunitrans where member=members.number and session='"+getSession()+"') AS mbunikgs, (select year from sessions where num='"+getSession()+"') AS season,(select sum(amount)-COALESCE((select sum(ccarf_payable) from paymentsc where member=members.num and ccarf_payable is not null),0) Balance from ccarf where member=members.num) as otheradvances FROM members order by num asc";
            System.out.println(msq);
            PreparedStatement statement = db.prepareStatement(msq);


            // Execute the query
            ResultSet resultSet = statement.executeQuery();





            while (resultSet.next()) {
                // Create a new JSON object for each row
                JSONObject resultJo = new JSONObject();

                // Populate the JSON object with column values
                resultJo.put("num", resultSet.getLong("num"));
                //resultJo.put("factory", resultSet.getString("factory"));
                resultJo.put("membername", resultSet.getString("membername"));
                resultJo.put("memberid", resultSet.getString("memberid"));
                resultJo.put("phone", resultSet.getString("phone"));
                try {
                    resultJo.put("trees", resultSet.getInt("trees"));
                } catch (Exception ignored) {
                }
                resultJo.put("altphone", resultSet.getString("altphone"));
                resultJo.put("Bank", resultSet.getString("Bank"));
                resultJo.put("baccount", resultSet.getString("baccount"));
                resultJo.put("cherrykgs", resultSet.getDouble("cherrykgs"));
                resultJo.put("mbunikgs", resultSet.getDouble("mbunikgs"));
                resultJo.put("season", resultSet.getString("season"));
                resultJo.put("otheradvances",resultSet.getDouble("otheradvances"));

                // Add the JSON object to the JSON array
                //jsonArray.add(resultJo);
                System.out.println(resultJo);
                jsonArray.put(resultJo);
            }

        }catch(SQLException gwgfgwe)
        {
        System.out.println(gwgfgwe);
        }
// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.

            String auth=getAuth();
            if(auth!=null&&!auth.equals("not authorized"))
            {
                feedData(auth,jsonArray);
            }
            else
            {
                System.out.println(auth);
            }


    }


    public static int getSession()
    {
        int sess=110001;
        try{
            ResultSet op=db.createStatement().executeQuery("select session from control where kaki=100037");
            op.next();
            if(!(op.getString(1)==null))
            {
                sess=Integer.valueOf(op.getString(1));
            }
        }catch(SQLException bui)
        {
            System.out.println(bui);
        }
        return sess;
    }



    public static String getAuth() {
        String factoryname = "MainFactory";
        String passcode = "bazubigman";
        String authKey = null;

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost request = new HttpPost(serverurl + "/loadData/registerfactory");


            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("factoryname", factoryname));
            params.add(new BasicNameValuePair("passphrase", passcode));

            request.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

            System.out.println(request);

            org.apache.http.HttpResponse response = httpClient.execute(request);

            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    authKey = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                }
            } else {
                // Handle error response
                System.out.println(response);
            }
        } catch (IOException e) {
            // Handle IO exception
            e.printStackTrace();
        }

        return authKey;
    }


    public static void feedData(String factoryauth, JSONArray data) {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost request = new HttpPost(serverurl + "/loadData/feed");


            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("factoryauth", factoryauth));
            params.add(new BasicNameValuePair("data", JSONObject.valueToString( data)));

            String jsonString = data.toString();
            JSONArray jsonArray = new JSONArray(jsonString);


            request.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

            System.out.println(request);

            org.apache.http.HttpResponse response = httpClient.execute(request);

            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    System.out.println(EntityUtils.toString(entity, StandardCharsets.UTF_8));
                }
            } else {
                // Handle error response
                System.out.println(response);
            }
        } catch (IOException e) {
            // Handle IO exception
            e.printStackTrace();
        }
    }

}