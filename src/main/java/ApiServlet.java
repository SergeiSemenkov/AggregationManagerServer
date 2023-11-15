import java.io.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.net.*;
import java.sql.*;

import java.lang.Class;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jdk.nashorn.internal.runtime.ParserException;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

public class ApiServlet  extends HttpServlet {
    //private static final String POSTGRESSQL_URL = "jdbc:postgresql://postgres:5432/postgres?stringtype=unspecified&user=postgres&password=postgres";
    //private static final String CLICKHOUSE_URL = "jdbc:clickhouse://10.51.98.64:8123/default?user=semenkovs&password=hbsFhYMK";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException  {

        UserInfo userInfo = null;
        if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
            userInfo = getUserInfo(request);
            if (userInfo == null || !userInfo.getIsAllowedToReadData()) {
//            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
//            return;
                throw new RuntimeException("User is not authorized to get data.");
            }
        }

        String pathInfo = request.getPathInfo();
        //System.out.println("Hello");
        String[] parts = pathInfo.split("/");
        String result = "";

        if(parts.length > 2 && parts[1].equals("v1")) {
            if(parts[2].equals("aggregations")) {
                response.setContentType("application/json");
                if(parts.length > 4) {
                    if(parts[4].equals("run-status")) {
                        //get start_nifi_process_id
                        String start_nifi_process_id = getStartNifiProcessIdFromDb(parts[3]);
                        //get state
                        result = getProcessorState(start_nifi_process_id);
                    }
                }
                else if(parts.length > 3) {
                    result = getAggregationList(false, parts[3]);
                }
                else {
                    result = getAggregationList(true, null);
                }
            }
            else if(parts[2].equals("users")) {
                if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
                    if (userInfo == null || !userInfo.getIsAdmin()) {
                        throw new RuntimeException("User is not authorized to change users data.");
                    }
                }

                response.setContentType("application/json");
                if(parts.length > 3) {
                    result = getUserList(false, parts[3]);
                }
                else {
                    result = getUserList(true, null);
                }
            }
            else if(parts[2].equals("events")) {
                response.setContentType("application/json");
                result = getEvents();
            }
            else if(parts[2].equals("settings")) {
                response.setContentType("application/json");
                result = JSONValue.toJSONString(getSettingsJson());
            }
            else if(parts[2].equals("templates")) {
                response.setContentType("application/json");
                result = getTemplates();
            }
            else if(parts[2].equals("nifi-processes")) {
                response.setContentType("application/json");
                result = getNifiProcesses();
            }
            else if(parts[2].equals("source-tables")) {
                response.setContentType("application/json");
                result = getSourceTables();
            }
            else if(parts[2].equals("source-table-columns")) {
                response.setContentType("application/json");
                result = getSourceTableColumns(
                        request.getParameter("database"),
                        request.getParameter("table")
                );
            }
        }

        PrintWriter out = response.getWriter();
        out.print(result);
    }

    // Create
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException  {
        //request.getParameter("page"),

        UserInfo userInfo = null;
        if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
            userInfo = getUserInfo(request);
            if (userInfo == null || !userInfo.getIsAllowedToChangeData()) {
                throw new RuntimeException("User is not authorized to post data.");
            }
        }

        String pathInfo = request.getPathInfo();
        String[] parts = pathInfo.split("/");
        String result = "";

        if(parts.length > 2 && parts[1].equals("v1")) {
            if(parts[2].equals("aggregations")) {

                StringBuffer jb = new StringBuffer();
                String line = null;
                BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);

                try {
                    JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());

                    String start_nifi_process_id = (String)json.get("start_nifi_process_id");
                    String aggregation_name = (String)json.get("aggregation_name");
                    String table_name = (String)json.get("table_name");
                    String temp_table_name = table_name + "_temp";

                    boolean is_generated_nifi_process = (Boolean)json.get("is_generated_nifi_process");

                    if(is_generated_nifi_process) {
                        //get clickhouse query
                        String query = (String) json.get("query");
//                        String createTableQuery = "createTableQuery stub";
//                        createTableQuery = GetCreateTableQuery(
//                                temp_table_name,
//                                (String) json.get("query"));


                        JSONObject settings = getSettingsJson();

                        //new template instance
                        // nifi_process_group_id, version
                        HashMap<String, String> processGroupParameters =
                                //nifiNewTemplateInstance((String)settings.get("default_template_id"));
                                nifiNewTemplateInstance((String)json.get("template"));

                        String processGroupId = processGroupParameters.get("nifi_process_group_id");

                        //set name
                        changeProcessGroupName(
                                processGroupId,
                                processGroupParameters.get("version"),
                                aggregation_name
                        );

                        start_nifi_process_id = this.getStartNifiProcessId(processGroupId);

                        String aggregationId = null;
                        try {
                            aggregationId = CreateAggregationTableRow(
                                    aggregation_name,
                                    table_name,
                                    (String) json.get("query"),
                                    new java.sql.Timestamp(System.currentTimeMillis()),
                                    (String) json.get("scheduling_strategy"),
                                    (String) json.get("scheduling_period"),
                                    processGroupId,
                                    start_nifi_process_id,
                                    is_generated_nifi_process
                            );

                            JSONObject processGroupJSON = getProcessGroupJson(processGroupId);
                            String version = ((JSONObject) processGroupJSON.get("revision")).get("version").toString();
                            //change variables variables
                            changeProcessGroupVariables(
                                    processGroupId,
                                    version,
                                    aggregationId,
                                    query,
                                    table_name
                            );

                            //set scheduling
                            setProcessorScheduling(
                                    start_nifi_process_id,
                                    "0",
                                    (String) json.get("scheduling_strategy"),
                                    (String) json.get("scheduling_period")
                            );

                            //activate controllers
                            setControllerServicesState(processGroupId, "ENABLED");

                            waitControllerServicesState(processGroupId, "ENABLED");

                            //run process group
                            setProcessorGroupState(processGroupId, "RUNNING");
                        }
                        catch(Exception e) {
                            DeleteAggregationTableRow(aggregationId);
                            JSONObject processGroupJSON = getProcessGroupJson(processGroupId);
                            if (processGroupJSON != null) {
                                String version = ((JSONObject) processGroupJSON.get("revision")).get("version").toString();
                                setProcessorGroupState(processGroupId, "STOPPED");

                                setControllerServicesState(processGroupId, "DISABLED");
                                waitControllerServicesState(processGroupId, "DISABLED");

                                deleteProcessGroup(processGroupId, version);
                            }

                            throw e;
                        }
                    }
                    else {
                        JSONObject processJSON = getProcessorJson(start_nifi_process_id);
                        String processGroupId = ((JSONObject)processJSON.get("component")).get("parentGroupId").toString();

                        JSONObject processGroupJSON = getProcessGroupJson(processGroupId);
                        String version = ((JSONObject) processGroupJSON.get("revision")).get("version").toString();
                        //change variables variables
                        changeProcessGroupVariables(
                                processGroupId,
                                version,
                                start_nifi_process_id,
                                null,
                                table_name
                        );

                        result = CreateAggregationTableRow(
                                aggregation_name,
                                table_name,
                                (String)json.get("query"),
                                new java.sql.Timestamp(System.currentTimeMillis()),
                                (String)json.get("scheduling_strategy"),
                                (String)json.get("scheduling_period"),
                                processGroupId,
                                start_nifi_process_id,
                                is_generated_nifi_process
                        );
                    }


                }catch (org.json.simple.parser.ParseException e){
                    response.setStatus(500);
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }catch (InterruptedException e){
                    response.setStatus(500);
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }

                response.setContentType("text/plain");
            }
            else if(parts[2].equals("users")) {
                if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
                    if (userInfo == null || !userInfo.getIsAdmin()) {
                        throw new RuntimeException("User is not authorized to change users data.");
                    }
                }

                StringBuffer jb = new StringBuffer();
                String line = null;
                BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);

                try {
                    JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());

                    String user_name = (String)json.get("user_name");
                    boolean is_admin = (Boolean)json.get("is_admin");
                    boolean is_power_user = (Boolean)json.get("is_power_user");

                    result = CreateUserTableRow(
                            user_name,
                            is_admin,
                            is_power_user
                    );
                }catch (org.json.simple.parser.ParseException e){
                    response.setStatus(500);
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }

                response.setContentType("text/plain");
            }
            else if(parts[2].equals("wizard-query")) {
                response.setContentType("application/json");
                StringBuffer jb = new StringBuffer();
                String line = null;
                BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);

                try {
                    JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());

                    result = getWizardQuery(json);
                }catch (org.json.simple.parser.ParseException e){
                    response.setStatus(500);
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }
            }
            else if(parts[2].equals("query-performance")) {
                response.setContentType("application/json");
                StringBuffer jb = new StringBuffer();
                String line = null;
                BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);

                try {
                    JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());

                    result = getQueryPerformance(json);
                }catch (org.json.simple.parser.ParseException e){
                    response.setStatus(500);
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }
            }
        }

        PrintWriter out = response.getWriter();
        out.print(result);
    }

    // Update
    @Override
    public void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException  {

        UserInfo userInfo = null;
        if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
            userInfo = getUserInfo(request);
            if (userInfo == null || !userInfo.getIsAllowedToChangeData()) {
                throw new RuntimeException("User is not authorized to post data.");
            }
        }

        String pathInfo = request.getPathInfo();
        //System.out.println("Hello");
        String[] parts = pathInfo.split("/");
        String result = "";

        if(parts.length > 2 && parts[1].equals("v1")) {
            if(parts.length > 3 && parts[2].equals("aggregations")) {
                StringBuffer jb = new StringBuffer();
                String line = null;
                BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);

                if(parts.length > 4) {
                    if(parts[4].equals("run-status")) {
                        try {
                            JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());
                            String state = (String) json.get("state");
                            String start_nifi_process_id = getStartNifiProcessIdFromDb(parts[3]);

                            String statusAsJson = getProcessorState(start_nifi_process_id);
                            json = (JSONObject) JSONValue.parseWithException(statusAsJson);
                            String currentStatus = (String)json.get("state");

                            if(!currentStatus.equals(state)) {
                                String version = getProcessorVersion(start_nifi_process_id);
                                if (
                                        currentStatus.equals("DISABLED")
                                        || state.equals("DISABLED")
                                ) {
                                    setProcessorState(start_nifi_process_id, version, "STOPPED");
                                    waitProcessorState(start_nifi_process_id, "STOPPED");
                                }
                                version = getProcessorVersion(start_nifi_process_id);
                                setProcessorState(start_nifi_process_id, version, state);
                                if(!state.equals("RUN_ONCE")) {
                                    waitProcessorState(start_nifi_process_id, state);
                                }
                            }

                        } catch (org.json.simple.parser.ParseException e) {
                            response.setStatus(500);
                            response.setContentType("text/plain");
                            result = e.getMessage();
                            e.printStackTrace();
                            System.out.println(e.getMessage());
                        }
                        catch (InterruptedException e) {
                            response.setStatus(500);
                            response.setContentType("text/plain");
                            result = e.getMessage();
                            e.printStackTrace();
                            System.out.println(e.getMessage());
                        }
                    }
                    else if(parts[4].equals("reset")) {
                        try {
                            JSONArray aggregationsJson = getAggregationListJson(false, parts[3]);
                            JSONObject aggregationJSON = (JSONObject)aggregationsJson.get(0);

                            boolean is_generated_nifi_process = aggregationJSON.get("is_generated_nifi_process").equals("t");

                            if(is_generated_nifi_process) {
                                //Check if exists
                                String process_group_id = (String) aggregationJSON.get("process_group_id");
                                JSONObject processGroupJSON = getProcessGroupJson(process_group_id);
                                if (processGroupJSON != null) {

                                    setProcessorGroupState(process_group_id, "STOPPED");

                                    ArrayList<JSONObject> connections = getProcessGroupConnectionsRecursive(process_group_id);
                                    for(JSONObject jsonObject: connections) {
                                        dropConnectionRequest((String)jsonObject.get("id"));
                                    }

                                    String start_nifi_process_id = (String)aggregationJSON.get("start_nifi_process_id");
                                    String version = getProcessorVersion(start_nifi_process_id);
                                    setProcessorState(
                                            start_nifi_process_id,
                                            version,
                                            "DISABLED");

                                    setProcessorGroupState(process_group_id, "RUNNING");
                                }
                            }
                        }catch (org.json.simple.parser.ParseException e){
                            response.setStatus(500);
                            result = e.getMessage();
                            e.printStackTrace();
                            System.out.println(e.getMessage());
                        }
                    }
                }
                else {

                    try {
                        JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());
                        String aggregation_name = (String)json.get("aggregation_name");
                        String table_name = (String)json.get("table_name");
                        String temp_table_name = table_name + "_temp";

                        JSONArray aggregationsJson = getAggregationListJson(false, parts[3]);
                        JSONObject aggregationJSON = (JSONObject)aggregationsJson.get(0);

                        String processGroupId = (String)aggregationJSON.get("process_group_id");
                        boolean is_generated_nifi_process = aggregationJSON.get("is_generated_nifi_process").equals("t");
                        String start_nifi_process_id = (String)aggregationJSON.get("start_nifi_process_id");
                        String start_nifi_process_version = getProcessorVersion(start_nifi_process_id);
                        setProcessorState(
                                start_nifi_process_id,
                                start_nifi_process_version,
                                "STOPPED");

                        UpdateAggregationTableRow(
                                parts[3],
                                (String) json.get("aggregation_name"),
                                (String) json.get("table_name"),
                                (String) json.get("query"),
                                (String) json.get("scheduling_strategy"),
                                (String) json.get("scheduling_period"),
                                start_nifi_process_id,
                                //(String) json.get("start_nifi_process_id"),
                                is_generated_nifi_process
                                //(Boolean) json.get("is_generated_nifi_process)"
                        );

                        start_nifi_process_version = getProcessorVersion(start_nifi_process_id);

                        //set scheduling
                        setProcessorScheduling(
                                start_nifi_process_id,
                                start_nifi_process_version,
                                (String)json.get("scheduling_strategy"),
                                (String)json.get("scheduling_period")
                        );

                        System.out.println(is_generated_nifi_process);
                        if(is_generated_nifi_process) {
                            //get clickhouse query
                            String query = (String) json.get("query");
//                            String createTableQuery = "createTableQuery stub";
//                            createTableQuery = GetCreateTableQuery(
//                                    temp_table_name,
//                                    (String) json.get("query"));

                            JSONObject processGroupJSON = getProcessGroupJson(processGroupId);
                            if (processGroupJSON != null) {
                                String version = ((JSONObject) processGroupJSON.get("revision")).get("version").toString();

                                //set name
                                changeProcessGroupName(
                                        processGroupId,
                                        version,
                                        aggregation_name
                                );

                                setProcessorGroupState(processGroupId, "STOPPED");

                                processGroupJSON = getProcessGroupJson(processGroupId);
                                version = ((JSONObject) processGroupJSON.get("revision")).get("version").toString();
                                //change variables variables
                                changeProcessGroupVariables(
                                        processGroupId,
                                        version,
                                        parts[3],
                                        query,
                                        table_name
                                );

                                version = getProcessorVersion(start_nifi_process_id);
                                setProcessorState(
                                        start_nifi_process_id,
                                        version,
                                        "DISABLED");

                                setProcessorGroupState(processGroupId, "RUNNING");

                            }
                        }
                    } catch (org.json.simple.parser.ParseException e) {
                        response.setStatus(500);
                        response.setContentType("text/plain");
                        result = e.getMessage();
                        e.printStackTrace();
                        System.out.println(e.getMessage());
                    }
                }

                response.setContentType("text/plain");
            }
            else if(parts.length > 3 && parts[2].equals("users")) {
                if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
                    if (userInfo == null || !userInfo.getIsAdmin()) {
                        throw new RuntimeException("User is not authorized to change users data.");
                    }
                }
                StringBuffer jb = new StringBuffer();
                String line = null;
                BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);

                try {
                    JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());
                    String user_name = (String)json.get("user_name");
                    boolean is_admin = (Boolean)json.get("is_admin");
                    boolean is_power_user = (Boolean)json.get("is_power_user");

                    UpdateUserTableRow(
                            parts[3],
                            user_name,
                            is_admin,
                            is_power_user
                    );


                } catch (org.json.simple.parser.ParseException e) {
                    response.setStatus(500);
                    response.setContentType("text/plain");
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }

                response.setContentType("text/plain");
            }
            else if(parts[2].equals("settings")) {

                StringBuffer jb = new StringBuffer();
                String line = null;
                BufferedReader reader = request.getReader();
                while ((line = reader.readLine()) != null)
                    jb.append(line);

                try {
                    JSONObject json = (JSONObject) JSONValue.parseWithException(jb.toString());

                    UpdateSettingsTableRow(
                            (String) json.get("default_template_id")
                    );

                } catch (org.json.simple.parser.ParseException e) {
                    response.setStatus(500);
                    response.setContentType("text/plain");
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }

            }
        }

        PrintWriter out = response.getWriter();
        out.print(result);
    }

    @Override
    public void doDelete(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException, ParserException {

        UserInfo userInfo = null;
        if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
            userInfo = getUserInfo(request);
            if (userInfo == null || !userInfo.getIsAllowedToChangeData()) {
                throw new RuntimeException("User is not authorized to post data.");
            }
        }

        String pathInfo = request.getPathInfo();
        //System.out.println("Hello");
        String[] parts = pathInfo.split("/");
        String result = "";

        if(parts.length > 3 && parts[1].equals("v1")) {
            if(parts[2].equals("aggregations")) {

                try {
                    JSONArray aggregationsJson = getAggregationListJson(false, parts[3]);
                    JSONObject aggregationJSON = (JSONObject)aggregationsJson.get(0);

                    boolean is_generated_nifi_process = aggregationJSON.get("is_generated_nifi_process").equals("t");
                    boolean generated_nifi_process_deleted = false;
                    if(is_generated_nifi_process) {
                        try {
                            //Need to Delete
                            //Check if exists
                            String process_group_id = (String) aggregationJSON.get("process_group_id");
                            JSONObject processGroupJSON = getProcessGroupJson(process_group_id);
                            if (processGroupJSON != null) {
                                String version = ((JSONObject) processGroupJSON.get("revision")).get("version").toString();
                                setProcessorGroupState(process_group_id, "STOPPED");

                                setControllerServicesState(process_group_id, "DISABLED");
                                waitControllerServicesState(process_group_id, "DISABLED");

                                ArrayList<JSONObject> connections = getProcessGroupConnectionsRecursive(process_group_id);
                                for(JSONObject jsonObject: connections) {
                                    dropConnectionRequest((String)jsonObject.get("id"));
                                }

                                deleteProcessGroup(process_group_id, version);
                            }
                            generated_nifi_process_deleted = true;
                        }
                        catch(InterruptedException e) {

                        }
                    }

                    if(!is_generated_nifi_process || generated_nifi_process_deleted) {
                        DeleteAggregationTableRow(parts[3]);
                        DeleteEventTableRows(parts[3]);
                    }
                    else {
                        throw new RuntimeException("Error while deleting generated nifi process.");
                    }
                }catch (org.json.simple.parser.ParseException e){
                    response.setStatus(500);
                    result = e.getMessage();
                    e.printStackTrace();
                    System.out.println(e.getMessage());
                }

                response.setContentType("text/plain");
            }
            else if(parts[2].equals("users")) {
                if(System.getenv("KEYCLOAK_AUTH_ENABLED") != null && System.getenv("KEYCLOAK_AUTH_ENABLED").equals("true")) {
                    if (userInfo == null || !userInfo.getIsAdmin()) {
                        throw new RuntimeException("User is not authorized to change users data.");
                    }
                }

                DeleteUserTableRow(parts[3]);

                response.setContentType("text/plain");
            }
        }

        PrintWriter out = response.getWriter();
        out.print(result);
    }

    private JSONArray getAggregationListJson(Boolean all, String id) {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String query = "select a.*, concat(a.scheduling_strategy, ': ', a.scheduling_period) schedule, e3.date_time last_data_update, e2.event_type last_event  from am.aggregations a\n" +
                    "left join \n" +
                    "(select e.aggregation_id aggregation_id, max(e.date_time) max_date_time from am.events e group by e.aggregation_id) max_event_date\n" +
                    "on a.id = max_event_date.aggregation_id\n" +
                    "left join am.events e2  on e2.aggregation_id = a.id and e2.date_time = max_event_date.max_date_time\n" +
                    "left join \n" +
                    "(select e.aggregation_id aggregation_id, max(e.date_time) max_date_time from am.events e where e.event_type = 'end' group by e.aggregation_id) max_end_event_date\n" +
                    "on a.id = max_end_event_date.aggregation_id\n" +
                    "left join am.events e3  on e3.aggregation_id = a.id and e3.date_time = max_end_event_date.max_date_time";

            PreparedStatement prep;
            if(all){
                prep = conn.prepareStatement(query);
            }
            else {
                query += " WHERE a.id = ?";
                prep = conn.prepareStatement(query);
                prep.setString(1, id);
            }
            ResultSet rs = prep.executeQuery();
            JSONArray jsonArray = (JSONArray)getJsonArray(rs);
            for(int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject =  (JSONObject)jsonArray.get(i);
                String currentStatus = null;
                try {
                    String statusAsJson = getProcessorState((String)jsonObject.get("start_nifi_process_id"));
                    JSONObject json = (JSONObject) JSONValue.parseWithException(statusAsJson);
                    currentStatus = (String)json.get("state");
                }
                catch(Exception e) {
                    currentStatus = "UNKNOWN";
                }
                ((JSONObject)jsonArray.get(i)).put("current_status", currentStatus);
            }
            return jsonArray;
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String getAggregationList(Boolean all, String id) {
        JSONArray jsonArray = getAggregationListJson(all, id);
        if(all) {
            return JSONValue.toJSONString(jsonArray);
        }
        else if(jsonArray.size() > 0) {
            return JSONValue.toJSONString(jsonArray.get(0));
        }
        else {
            return "";
        }
    }

    private JSONArray getUserListJson(Boolean all, String id) {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String query = "select * from am.users";

            PreparedStatement prep;
            if(all){
                prep = conn.prepareStatement(query);
            }
            else {
                query += " WHERE id = ?";
                prep = conn.prepareStatement(query);
                prep.setString(1, id);
            }
            ResultSet rs = prep.executeQuery();
            JSONArray jsonArray = (JSONArray)getJsonArray(rs);
            return jsonArray;
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private JSONArray getUserListJsonByUserName(String user_name) {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String query = "select * from am.users WHERE user_name = ?";

            PreparedStatement prep;
            prep = conn.prepareStatement(query);
            prep.setString(1, user_name);

            ResultSet rs = prep.executeQuery();
            JSONArray jsonArray = (JSONArray)getJsonArray(rs);
            return jsonArray;
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String getUserList(Boolean all, String id) {
        JSONArray jsonArray = getUserListJson(all, id);
        if(all) {
            return JSONValue.toJSONString(jsonArray);
        }
        else if(jsonArray.size() > 0) {
            return JSONValue.toJSONString(jsonArray.get(0));
        }
        else {
            return "";
        }
    }

    private String getEvents() {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select e.*, a.aggregation_name from am.events e left join am.aggregations a on e.aggregation_id = a.id order by e.date_time desc limit 1001");
            JSONArray jsonArray = getJsonArray(rs);
            return JSONValue.toJSONString(jsonArray);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private JSONObject getSettingsJson() {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select * from am.settings limit 1");
            JSONArray jsonArray = getJsonArray(rs);
            if(jsonArray.size() > 0) {
                try {
                    HashMap<String, Object> result = getTemplatesMap();

                    if(result.containsKey(((JSONObject)jsonArray.get(0)).get("default_template_id"))) {
                        JSONObject template = (JSONObject)result.get(((JSONObject)jsonArray.get(0)).get("default_template_id"));
                        ((JSONObject)jsonArray.get(0)).put(
                                "default_template_name",
                                ((JSONObject)template.get("template")).get("name"));
                    }
                    else {
                        ((JSONObject)jsonArray.get(0)).put("default_template_name", "UNKNOWN");
                    }
                }
                catch(Exception e) {
                    ((JSONObject)jsonArray.get(0)).put("default_template_name", "UNKNOWN");
                }

                return (JSONObject)jsonArray.get(0);
            }
            else {
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String getTemplates() {
        try {
            HashMap<String, Object> result = getTemplatesMap();

            JSONArray arr = new JSONArray();

            for (Map.Entry<String,Object> entry : result.entrySet()) {
                JSONObject template = (JSONObject)entry.getValue();

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("id", template.get("id"));
                jsonObject.put("name", ((JSONObject)template.get("template")).get("name"));

                arr.add(jsonObject);
            }

            return JSONValue.toJSONString(arr);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

        return null;
    }

    private String getNifiProcesses()throws IOException {
        String result = "[]";

        URL url = new URL("http://nifi:8080/nifi-api/flow/search-results?q=IsAggregationManagerStart");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            result = response.toString();
//            System.out.println(result);

            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(result);
                JSONObject searchResultsDTO = (JSONObject)json.get("searchResultsDTO");
                if(searchResultsDTO != null) {
                    JSONArray processorResults = (JSONArray) searchResultsDTO.get("processorResults");
                    JSONArray resultArray = new JSONArray();
                    for(int i=0; i<processorResults.size(); i++){
                        JSONObject obj = new JSONObject();
                        obj.put("id", ((JSONObject)processorResults.get(i)).get("id"));
                        obj.put("name", ((JSONObject)processorResults.get(i)).get("name"));
                        resultArray.add(obj);
                    }
                    result = JSONValue.toJSONString(resultArray);
                }

            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return result;
    }

    private String CreateAggregationTableRow(
            String aggregation_name,
            String table_name,
            String query,
            Timestamp last_schema_update,
            String scheduling_strategy,
            String scheduling_period,
            String process_group_id,
            String start_nifi_process_id,
            Boolean is_generated_nifi_process
            ) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String insertQuery = "INSERT INTO am.aggregations (" +
                    "aggregation_name, " +
                    "table_name, " +
                    "query, " +
                    "last_schema_update, " +
                    "scheduling_strategy, " +
                    "scheduling_period, " +
                    "process_group_id, " +
                    "start_nifi_process_id, " +
                    "is_generated_nifi_process) " +
                    "VALUES(?,?,?,?,?,?,?,?,?)";
            PreparedStatement prep = conn.prepareStatement(insertQuery ,Statement.RETURN_GENERATED_KEYS);
            prep.setString(1, aggregation_name);
            prep.setString(2, table_name);
            prep.setString(3, query);
            prep.setTimestamp(4, last_schema_update);
            prep.setString(5, scheduling_strategy);
            prep.setString(6, scheduling_period);
            prep.setString(7, process_group_id);
            prep.setString(8, start_nifi_process_id);
            if(is_generated_nifi_process == null) {
                prep.setNull(9, java.sql.Types.NULL);
            }
            else {
                prep.setBoolean(9, is_generated_nifi_process);
            }
            prep.executeUpdate();
            ResultSet rs = prep.getGeneratedKeys();
            if(rs.next() && rs != null){
                return rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String UpdateAggregationTableRow(
            String id,
            String aggregation_name,
            String table_name,
            String query,
            String scheduling_strategy,
            String scheduling_period,
            String start_nifi_process_id,
            Boolean is_generated_nifi_process
    ) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String updateQuery = "UPDATE am.aggregations SET " +
                    "aggregation_name = ?, " +
                    "table_name = ?, " +
                    "query = ?, " +
                    "scheduling_strategy = ?, " +
                    "scheduling_period = ?, " +
                    "start_nifi_process_id = ?, " +
                    "is_generated_nifi_process = ? " +
                    "WHERE id = ?";
            PreparedStatement prep = conn.prepareStatement(updateQuery);
            prep.setString(1, aggregation_name);
            prep.setString(2, table_name);
            prep.setString(3, query);
            prep.setString(4, scheduling_strategy);
            prep.setString(5, scheduling_period);
            prep.setString(6, start_nifi_process_id);
            if(is_generated_nifi_process == null) {
                prep.setNull(7, java.sql.Types.NULL);
            }
            else {
                prep.setBoolean(7, is_generated_nifi_process);
            }
            prep.setString(8, id);
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String CreateUserTableRow(
            String user_name,
            Boolean is_admin,
            Boolean is_power_user
    ) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String insertQuery = "INSERT INTO am.users (" +
                    "user_name, " +
                    "is_admin, " +
                    "is_power_user) " +
                    "VALUES(?,?,?)";
            PreparedStatement prep = conn.prepareStatement(insertQuery ,Statement.RETURN_GENERATED_KEYS);
            prep.setString(1, user_name);
            if(is_admin == null) {
                prep.setNull(2, java.sql.Types.NULL);
            }
            else {
                prep.setBoolean(2, is_admin);
            }
            if(is_power_user == null) {
                prep.setNull(3, java.sql.Types.NULL);
            }
            else {
                prep.setBoolean(3, is_power_user);
            }
            prep.executeUpdate();
            ResultSet rs = prep.getGeneratedKeys();
            if(rs.next() && rs != null){
                return rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String UpdateUserTableRow(
            String id,
            String user_name,
            Boolean is_admin,
            Boolean is_power_user
    ) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String updateQuery = "UPDATE am.users SET " +
                    "user_name = ?, " +
                    "is_admin = ?, " +
                    "is_power_user = ? " +
                    "WHERE id = ?";
            PreparedStatement prep = conn.prepareStatement(updateQuery);
            prep.setString(1, user_name);
            if(is_admin == null) {
                prep.setNull(2, java.sql.Types.NULL);
            }
            else {
                prep.setBoolean(2, is_admin);
            }
            if(is_power_user == null) {
                prep.setNull(3, java.sql.Types.NULL);
            }
            else {
                prep.setBoolean(3, is_power_user);
            }
            prep.setString(4, id);
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String UpdateSettingsTableRow(
            String default_template_id
    ) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String updateQuery = "UPDATE am.settings SET " +
                    "default_template_id = ?";
            PreparedStatement prep = conn.prepareStatement(updateQuery);
            prep.setString(1, default_template_id);
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String DeleteAggregationTableRow(String id) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String updateQuery = "DELETE FROM am.aggregations WHERE id = ?";
            PreparedStatement prep = conn.prepareStatement(updateQuery);
            prep.setString(1, id);
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String DeleteUserTableRow(String id) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String updateQuery = "DELETE FROM am.users WHERE id = ?";
            PreparedStatement prep = conn.prepareStatement(updateQuery);
            prep.setString(1, id);
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String DeleteEventTableRows(String aggregation_id) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String updateQuery = "DELETE FROM am.events WHERE aggregation_id = ?";
            PreparedStatement prep = conn.prepareStatement(updateQuery);
            prep.setString(1, aggregation_id);
            prep.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private JSONArray getJsonArray(ResultSet rs) throws SQLException {
        JSONArray arr = new JSONArray();

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        while (rs.next()) {
            JSONObject obj = new JSONObject();
            for(int i = 1; i <= columnCount; i++) {
                obj.put(rsmd.getColumnName(i), rs.getString(i));
            }
            arr.add(obj);
        }

        return arr;
    }

/*
    private String GetCreateTableQuery(String temptableName, String sourceQuery) throws SQLException, ClassNotFoundException {
        String result = "CREATE TABLE " + temptableName + "\n ( \n";
        Connection conn = null;

        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        conn = DriverManager.getConnection(System.getenv("CLICKHOUSE_URL"));
        String getColumnQuery = "SELECT * FROM (" + sourceQuery + ") LIMIT 0";
        PreparedStatement prep = conn.prepareStatement(getColumnQuery);

        ResultSet rs = prep.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        boolean first = true;
        String columnList = "";
        String primaryKeyList = "";
        for(int i = 1; i <= rsmd.getColumnCount(); i++) {
            if(!first) {
                columnList += ", ";
                primaryKeyList += ", ";
            }
            columnList += "\"" + rsmd.getColumnName(i) + "\" " + rsmd.getColumnTypeName(i) + "\n";
            primaryKeyList += "\"" + rsmd.getColumnName(i) + "\"\n";
            first = false;
        }
        result += columnList  + ", PRIMARY KEY (\n" + primaryKeyList + "))\n ENGINE = MergeTree \n AS \n";
        result += sourceQuery;

        //System.out.println(result);
        return result;
    }
*/

    private HashMap<String, String> nifiNewTemplateInstance(String templateId) throws IOException,
            org.json.simple.parser.ParseException
    {
        HashMap<String, String> resultMap = new HashMap<String, String>();

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/root/template-instance");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
        osw.write("{\"originX\": 0.0, \"originY\": 0.0,\"templateId\": \"" + templateId + "\"}");
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream

        con.connect();

        int responseCode = con.getResponseCode();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        String result = response.toString();

        // print result
        System.out.println("GET Response Code :: " + responseCode);
        System.out.println(result);

        if (responseCode == HttpURLConnection.HTTP_OK || responseCode == 201) { // success
            JSONObject json = (JSONObject) JSONValue.parseWithException(result);
            JSONObject flowO = (JSONObject)json.get("flow");
            JSONArray processGroupsA = (JSONArray)flowO.get("processGroups");
            JSONObject processGroupO = (JSONObject)processGroupsA.get(0);
            resultMap.put("nifi_process_group_id", (String)processGroupO.get("id"));
            JSONObject revisionO = (JSONObject)processGroupO.get("revision");
            resultMap.put("version", revisionO.get("version").toString());

        } else {
            System.out.println("POST request did not work.");
        }

        con.disconnect();
        return resultMap;
    }
    //changeProcessGroupName

    private void changeProcessGroupName(String processGroupId, String version, String name) throws IOException,
            org.json.simple.parser.ParseException
    {
//        String result = null;

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/" + processGroupId);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("PUT");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
        String data = "{\"revision\": {\"version\": " + version + "},\"component\": {\"id\": \"" + processGroupId + "\", \"name\": \"" + name + "\"}}";
        osw.write(data);
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream

        con.connect();

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);

//        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
//        String inputLine;
//        StringBuffer response = new StringBuffer();
//
//        while ((inputLine = in.readLine()) != null) {
//            response.append(inputLine);
//        }
//        in.close();
//
//        String result = response.toString();
//
//        // print result
//        System.out.println(result);

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
        } else {
            System.out.println("POST request did not work.");
        }

        con.disconnect();
//        return result;
    }

    private void changeProcessGroupVariables(
            String processGroupId,
            String version,
            String aggregation_id,
            String query,
            String table_name
            ) throws IOException,
            org.json.simple.parser.ParseException
    {
//        String result = null;

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/" + processGroupId + "/variable-registry");
        System.out.println(url.toString());
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("PUT");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();


        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");

        JSONObject queryVariable = new JSONObject();
        queryVariable.put("name", "query");
        queryVariable.put("value", query);

//        String data = "{\"processGroupRevision\": {\"version\": " + version + "},\"variableRegistry\": {\"variables\": [" +
//                "{\"variable\": {\"name\": \"aggregation_id\",\"value\": \"" + aggregation_id + "\"}}," +
//                "{\"variable\": {\"name\": \"create_table_query\",\"value\": \"" + create_table_query + "\"}}," +
//                "{\"variable\": {\"name\": \"table_name\",\"value\": \"" + table_name + "\"}}," +
//                "{\"variable\": {\"name\": \"temp_table_name\",\"value\": \""+ temp_table_name +"\"}}]," +
//                "\"processGroupId\": \"" + processGroupId +"\"}}";

        String data = "{\"processGroupRevision\": {\"version\": " + version + "},\"variableRegistry\": {\"variables\": [" +
                "{\"variable\": {\"name\": \"aggregation_id\",\"value\": \"" + aggregation_id + "\"}}," +
                "{\"variable\": " + queryVariable.toString() + "}," +
                "{\"variable\": {\"name\": \"table_name\",\"value\": \"" + table_name + "\"}}]," +
                "\"processGroupId\": \"" + processGroupId +"\"}}";

        System.out.println(data);
        osw.write(data);
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream


        con.connect();

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        String result = response.toString();
//
//        // print result
//        System.out.println(result);

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
        } else {
            System.out.println("POST request did not work.");
            throw new RuntimeException(result);
        }

        con.disconnect();
//        return result;
    }

    private HashMap<String, Object> getControllerServices(String processGroupId)throws IOException {
        HashMap<String, Object> result = new HashMap<String, Object>();

        URL url = new URL("http://nifi:8080/nifi-api/flow/process-groups/" + processGroupId + "/controller-services");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String resultResponse = response.toString();
//            System.out.println(resultResponse);

            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(resultResponse);
                JSONArray controllerServices = (JSONArray) json.get("controllerServices");
                for(int i=0; i<controllerServices.size(); i++){
                    result.put("id", (String)((JSONObject)controllerServices.get(i)).get("id"));
                    JSONObject status =  (JSONObject)((JSONObject)controllerServices.get(i)).get("status");
                    result.put("status", status.get("runStatus"));
                }
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return result;
    }

    private HashMap<String, Object> getTemplatesMap()throws IOException {
        HashMap<String, Object> result = new HashMap<String, Object>();

        URL url = new URL("http://nifi:8080/nifi-api/flow/templates");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String resultResponse = response.toString();
//            System.out.println(resultResponse);

            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(resultResponse);
                JSONArray templates = (JSONArray) json.get("templates");
                for(int i=0; i<templates.size(); i++){
                    result.put((String)((JSONObject)templates.get(i)).get("id"), templates.get(i));
                }
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return result;
    }

    private void waitControllerServicesState(String processGroupId, String state) throws IOException, InterruptedException {
        int delay = 0;
        boolean controllersAreSet = false;
        while(!controllersAreSet && delay < 60) {
            HashMap<String, Object> controllareStates = getControllerServices(processGroupId);
            controllersAreSet = true;
            for (int i = 0; i < controllareStates.size(); i++) {
                String runStatusString = (String)controllareStates.get("status");
                controllersAreSet = controllersAreSet && runStatusString.equals(state);
            }

            TimeUnit.SECONDS.sleep(5);
            delay += 5;
        }
        if(!controllersAreSet) {
            throw new RuntimeException("Error while setting controller services state.");
        }
    }

    private void waitProcessorState(String id, String state) throws IOException, InterruptedException, ParseException {
        int delay = 0;
        boolean processorIsSet = false;
        while(!processorIsSet && delay < 60) {
            String statusAsJson = getProcessorState(id);
            JSONObject json = (JSONObject) JSONValue.parseWithException(statusAsJson);
            String currentStatus = (String)json.get("state");
            processorIsSet = currentStatus.equals(state);

            TimeUnit.SECONDS.sleep(5);
            delay += 5;
        }
        if(!processorIsSet) {
            throw new RuntimeException("Error while setting processor state.");
        }
    }

    private void setControllerServicesState(String processGroupId, String state) throws IOException,
            org.json.simple.parser.ParseException
    {
//        String result = null;

        URL url = new URL("http://nifi:8080/nifi-api/flow/process-groups/" + processGroupId + "/controller-services");
        //System.out.println(url.toString());
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("PUT");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();


        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
        String data = "{\"id\": \"" + processGroupId + "\",\"state\": \"" + state + "\"}";
        //System.out.println(data);
        osw.write(data);
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream


        con.connect();

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);

//        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
//        String inputLine;
//        StringBuffer response = new StringBuffer();
//
//        while ((inputLine = in.readLine()) != null) {
//            response.append(inputLine);
//        }
//        in.close();
//
//        String result = response.toString();
//
//        // print result
//        System.out.println(result);

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
        } else {
            System.out.println("POST request did not work.");
        }

        con.disconnect();
//        return result;
    }

    private void setProcessorGroupState(String processGroupId, String state) throws IOException,
            org.json.simple.parser.ParseException
    {
//        String result = null;

        URL url = new URL("http://nifi:8080/nifi-api/flow/process-groups/" + processGroupId);
        //System.out.println(url.toString());
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("PUT");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();


        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
        String data = "{\"id\": \"" + processGroupId + "\",\"state\": \"" + state + "\"}";
        //System.out.println(data);
        osw.write(data);
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream


        con.connect();

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);

//        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
//        String inputLine;
//        StringBuffer response = new StringBuffer();
//
//        while ((inputLine = in.readLine()) != null) {
//            response.append(inputLine);
//        }
//        in.close();
//
//        String result = response.toString();
//
//        // print result
//        System.out.println(result);

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
        } else {
            System.out.println("POST request did not work.");
        }

        con.disconnect();
//        return result;
    }

    private String getStartNifiProcessId(String processGroupId)throws IOException {
        String result = null;

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/" + processGroupId + "/processors");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String resultResponse = response.toString();
//            System.out.println(resultResponse);

            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(resultResponse);
                JSONArray processorsJSONArray = (JSONArray) json.get("processors");
                for(int i=0; i<processorsJSONArray.size(); i++){
                    JSONObject processorJSONObject = (JSONObject)processorsJSONArray.get(i);
                    JSONObject componentJSONObject = (JSONObject)processorJSONObject.get("component");
                    JSONObject configJSONObject = (JSONObject)componentJSONObject.get("config");
                    JSONObject descriptorsJSONObject = (JSONObject)configJSONObject.get("descriptors");
                    JSONObject isAggregationManagerStart = (JSONObject)descriptorsJSONObject.get("IsAggregationManagerStart");
                    if(isAggregationManagerStart != null) {
                        return processorJSONObject.get("id").toString();
                    }
                }
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return result;
    }

    private void setProcessorScheduling (
            String processorId,
            String version,
            String schedulingStrategy,
            String schedulingPeriod) throws IOException,
            org.json.simple.parser.ParseException
    {
//        String result = null;

        URL url = new URL("http://nifi:8080/nifi-api/processors/" + processorId);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("PUT");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");

        JSONObject topJsonObject = new JSONObject();
        JSONObject versionJsonObject = new JSONObject();
        topJsonObject.put("revision", versionJsonObject);
        versionJsonObject.put("version", version);
        JSONObject componentJsonObject = new JSONObject();
        topJsonObject.put("component", componentJsonObject);
        componentJsonObject.put("id", processorId);
        JSONObject configJsonObject = new JSONObject();
        componentJsonObject.put("config", configJsonObject);
        configJsonObject.put("schedulingPeriod", schedulingPeriod);
        configJsonObject.put("schedulingStrategy", schedulingStrategy);

        String data = topJsonObject.toString();
        osw.write(data);
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream

        con.connect();

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        String result = response.toString();
//
//        // print result
//        System.out.println(result);

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
        } else {
            System.out.println("POST request did not work.");
            throw new RuntimeException(result);
        }

        con.disconnect();
//        return result;
    }

    String getSourceTables() {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            conn = DriverManager.getConnection(System.getenv("CLICKHOUSE_URL"));
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT database, name FROM system.tables");
            JSONArray jsonArray = getJsonArray(rs);
            return JSONValue.toJSONString(jsonArray);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    String getSourceTableColumns(String database, String table) {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            conn = DriverManager.getConnection(System.getenv("CLICKHOUSE_URL"));
            PreparedStatement prep = conn.prepareStatement("SELECT name, type, position  FROM system.columns WHERE database = ? AND table = ?");
            prep.setString(1, database);
            prep.setString(2, table);
            ResultSet rs = prep.executeQuery();
            JSONArray jsonArray = getJsonArray(rs);
            return JSONValue.toJSONString(jsonArray);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    String getWizardQuery(JSONObject json){
        JSONObject result = new JSONObject();

        String query = "SELECT \n";

        JSONArray columns = (JSONArray)json.get("columns");
        String columnLines = "";
        String groupByLines = "";
        for(int i = 0; i < columns.size(); i++) {
            JSONObject column = (JSONObject)columns.get(i);
            if(((String)column.get("type")).equals("group by")) {
                if(!columnLines.isEmpty()) {
                    columnLines += ",\n";
                }
                columnLines += (String)column.get("name");
                if(!groupByLines.isEmpty()) {
                    groupByLines += ",\n";
                }
                groupByLines += (String)column.get("name");
            }
            else if(((String)column.get("type")).equals("aggregate")) {
                if(!columnLines.isEmpty()) {
                    columnLines += ",\n";
                }
                columnLines += (String)column.get("aggregate_function") + "(" + (String)column.get("name") + ")";
            }
        }

        query += columnLines + "\n";

        query += "FROM \"" + (String)json.get("database") + "\".\"" + (String)json.get("table") + "\"";
        query += "\nGROUP BY " + groupByLines;

        result.put("query", query);
        return result.toString();
    }

    String getQueryPerformance(JSONObject json){
        JSONObject result = new JSONObject();
        String query = "SELECT count(*) FROM (" + (String)json.get("query") + ") AS T";

        Connection conn = null;
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            conn = DriverManager.getConnection(System.getenv("CLICKHOUSE_URL"));

            PreparedStatement prep;
            prep = conn.prepareStatement(query);
            ResultSet rs = prep.executeQuery();
            if(rs.next()) {
                result.put("rows_count", rs.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return result.toString();
    }

    private String getStartNifiProcessIdFromDb(String id) {
        Connection conn = null;
        JSONArray arr = new JSONArray();
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(System.getenv("POSTGRESSQL_URL"));
            String query = "select start_nifi_process_id from am.aggregations where id = ?";

            PreparedStatement prep;
            prep = conn.prepareStatement(query);
            prep.setString(1, id);
            ResultSet rs = prep.executeQuery();
            if(rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }
        finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String getProcessorState(String id) throws IOException {
        String result = "";

        URL url = new URL("http://nifi:8080/nifi-api/processors/" + id);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            result = response.toString();
//            System.out.println(result);

            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(result);
                JSONObject componentJSONObject = (JSONObject)json.get("component");

                JSONObject resultJSONObject = new JSONObject();
                resultJSONObject.put("state", componentJSONObject.get("state"));
                result = JSONValue.toJSONString(resultJSONObject);
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return result;
    }

    private void setProcessorState (
            String processorId,
            String version,
            String state) throws IOException
    {
//        String result = null;

        URL url = new URL("http://nifi:8080/nifi-api/processors/" + processorId + "/run-status");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        con.setRequestMethod("PUT");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");

        JSONObject topJsonObject = new JSONObject();
        JSONObject versionJsonObject = new JSONObject();
        topJsonObject.put("revision", versionJsonObject);
        versionJsonObject.put("version", version);
        topJsonObject.put("state", state);

        String data = topJsonObject.toString();
        osw.write(data);
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream

        con.connect();

        int responseCode = con.getResponseCode();
        System.out.println("PUT Response Code :: " + responseCode);

//        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
//        String inputLine;
//        StringBuffer response = new StringBuffer();
//
//        while ((inputLine = in.readLine()) != null) {
//            response.append(inputLine);
//        }
//        in.close();
//
//        String result = response.toString();
//
//        // print result
//        System.out.println(result);

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
        } else {
            System.out.println("POST request did not work.");
            throw new RuntimeException("Error while changing processor state.");
        }

        con.disconnect();
//        return result;
    }

    private String getProcessorVersion(String id) throws IOException {
        String result = "";

        URL url = new URL("http://nifi:8080/nifi-api/processors/" + id);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            result = response.toString();
//            System.out.println(result);


            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(result);
                JSONObject revisionJSONObject = (JSONObject)json.get("revision");
                result = revisionJSONObject.get("version").toString();
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return result;
    }

    private JSONObject getProcessorJson(String id) throws IOException {
        String result = "";

        URL url = new URL("http://nifi:8080/nifi-api/processors/" + id);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            result = response.toString();
//            System.out.println(result);

            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(result);
                return json;
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return null;
    }

    private JSONObject getProcessGroupJson(String id) throws IOException {
        String result = "";

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/" + id);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            result = response.toString();
            //System.out.println(result);

            try {
                JSONObject json = (JSONObject) JSONValue.parseWithException(result);
                return json;
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();
        return null;
    }

    private JSONArray deleteProcessGroup(String id, String version) throws IOException {

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/" + id + "?version=" + version);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("DELETE");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("DELETE Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String result = response.toString();
            //System.out.println(result);

        } else {
            System.out.println("DELETE request did not work.");
        }

        con.disconnect();

        return null;
    }


    private ArrayList<JSONObject> getProcessGroupConnectionsRecursive(String id) throws IOException {
        ArrayList<JSONObject> connections = new ArrayList<JSONObject>();

        JSONObject jsonConnections = getProcessGroupConnections(id);
        for(Object jsonConnection: (JSONArray)jsonConnections.get("connections")) {
            connections.add((JSONObject)jsonConnection);
        }
        JSONObject jsonProcessGroups = getProcessGroupProcessGroups(id);
        for(Object pocessGroup: (JSONArray)jsonProcessGroups.get("processGroups")) {
            JSONObject jsonProcessGroup = (JSONObject)pocessGroup;
            connections.addAll(getProcessGroupConnectionsRecursive((String)jsonProcessGroup.get("id")));
        }

        return connections;
    }

    private JSONObject getProcessGroupConnections(String id) throws IOException {

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/" + id + "/connections");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String result = response.toString();
            //System.out.println(result);

            try {
                return (JSONObject) JSONValue.parseWithException(result);
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();

        return null;
    }

    private JSONObject getProcessGroupProcessGroups(String id) throws IOException {

        URL url = new URL("http://nifi:8080/nifi-api/process-groups/" + id + "/process-groups");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String result = response.toString();
            //System.out.println(result);

            try {
                return (JSONObject) JSONValue.parseWithException(result);
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();

        return null;
    }

    // can be POST
    // /process-groups/{id}/empty-all-connections-requests
    private JSONArray dropConnectionRequest(String id) throws IOException {

        URL url = new URL("http://nifi:8080/nifi-api/flowfile-queues/" + id + "/drop-requests");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String result = response.toString();
            //System.out.println(result);

        } else {
            System.out.println("POST request did not work.");
        }

        con.disconnect();

        return null;
    }

    private JSONObject getKeyCloakTokenInfo(String token) throws IOException {

        //KEYCLOAK_AUTH_BASE_PATH=https://sso-st.dpd.ch/auth/
        //KEYCLOAK_AUTH_REALM=DPD
        URL url = new URL(
                System.getenv("KEYCLOAK_AUTH_BASE_PATH")
                        + "realms/"
                        + System.getenv("KEYCLOAK_AUTH_REALM")
                        + "/protocol/openid-connect/token/introspect"
        );

//        System.out.println("url:");
//        System.out.println(url.toString());

        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
//        con.setConnectTimeout(5000);
//        con.setReadTimeout(5000);

        OutputStream os = con.getOutputStream();
        OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");

        String data = "client_secret=" + System.getenv("KEYCLOAK_CLIENT_SECRET")
                + "&client_id=" + System.getenv("KEYCLOAK_CLIENT_ID")
                + "&token=" + token;
        osw.write(data);
        osw.flush();
        osw.close();
        os.close();  //don't forget to close the OutputStream

//        System.out.println("data:");
//        System.out.println(data);

        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);
        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // print result
            String result = response.toString();
            //System.out.println(result);

            try {
                return (JSONObject) JSONValue.parseWithException(result);
            }catch (org.json.simple.parser.ParseException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            }

        } else {
            System.out.println("GET request did not work.");
        }

        con.disconnect();

        return null;
    }

    private UserInfo getUserInfo(HttpServletRequest request) throws IOException {
        String jwttoken = request.getHeader("Authorization");
        if(jwttoken != null) {
            String[] tokenArray = jwttoken.split(" ");
            String token = tokenArray[1];

            JSONObject tokenInfo = getKeyCloakTokenInfo(token);

            if(tokenInfo != null) {
                UserInfo userInfo = new UserInfo();
                userInfo.tokenInfo = tokenInfo;
                if(tokenInfo.containsKey("active")) {
                    boolean activeValue = (boolean)tokenInfo.get("active");
                    if(activeValue) {
                        String username = (String)tokenInfo.get("username");
                        userInfo.userName = username;

                        JSONArray userList = getUserListJsonByUserName(username);

                        if(userList.size() > 0) {
//                                System.out.println("userRow :");
//                                System.out.println(userList.get(0).toString());
                            userInfo.userRow = (JSONObject)userList.get(0);
                            return userInfo;
                        }
                    }
                }
                return userInfo;
            }
        }

        return null;
    }

    private class UserInfo {
        public String userName;
        public JSONObject tokenInfo;
        public JSONObject userRow;
        public boolean getIsAdmin() {
            return (System.getenv("ADMIN_USERNAME") != null && !System.getenv("ADMIN_USERNAME").equals(userName))
                    ||
                    (userRow != null && userRow.get("is_admin") != null && userRow.get("is_admin").equals("t"));
        }
        public boolean getIsPowerUser() {
            return userRow != null && userRow.get("is_power_user") != null && userRow.get("is_power_user").equals("t");
        }
        public boolean getIsAllowedToReadData() {
            return getIsAdmin() || userRow != null;
        }
        public boolean getIsAllowedToChangeData() {
            return getIsAdmin() || getIsPowerUser();
        }
    }

}
