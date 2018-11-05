package com.blazingdb.calcite.cli;

import com.blazingdb.calcite.application.ApplicationContext;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DMLRequestMessage;
import com.google.gson.Gson;
import org.apache.calcite.plan.RelOptUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Generator {
    class CalciteParams {
        String[] columnNames;
        String[] types;
        String name;
        String dbName;
        String query;
        public CalciteParams() {
            // no-args constructor
        }
    }

    public static void main(String[] args){
        Gson gson = new Gson();
        String json = "";
        try {
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                json += sc.nextLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        CalciteParams params = gson.fromJson(json, CalciteParams.class);
        ApplicationContext.init(); //any api call initializes it actually
        List<String> columnNames = Arrays.asList(params.columnNames);
        List<String> types = Arrays.asList(params.types);
        String name = params.name;
        String dbName = params.dbName;

        try {
            DDLDropTableRequestMessage message = new DDLDropTableRequestMessage(name, dbName);
            ApplicationContext.getCatalogService().dropTable(message);
            ApplicationContext.updateContext();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            DDLCreateTableRequestMessage message = new DDLCreateTableRequestMessage(columnNames, types, name, dbName);
            ApplicationContext.getCatalogService().createTable(message);
            ApplicationContext.updateContext();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            String query = params.query;
            DMLRequestMessage requestPayload = new DMLRequestMessage(query);
            String logicalPlan  = RelOptUtil.toString(ApplicationContext.getRelationalAlgebraGenerator().getRelationalAlgebra(requestPayload.getQuery()));
            System.out.println("@JSON");
            System.out.println(logicalPlan);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}