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
    class Table {
        String[] columnNames;
        String[] columnTypes;
        String tableName;
        String dbName;
    }
    class CalciteParams {
        List<Table> tables;
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
        System.out.println("REGISTERING-TABLES");
        for(Table table : params.tables) {
            List<String> columnNames = Arrays.asList(table.columnNames);
            List<String> types = Arrays.asList(table.columnTypes);
            String name = table.tableName;
            String dbName = table.dbName;
            try {
                {
                    DDLDropTableRequestMessage message = new DDLDropTableRequestMessage(name, dbName);
                    ApplicationContext.getCatalogService().dropTable(message);
                    ApplicationContext.updateContext();
                }
                {
                    DDLCreateTableRequestMessage message = new DDLCreateTableRequestMessage(columnNames, types, name, dbName);
                    ApplicationContext.getCatalogService().createTable(message);
                    ApplicationContext.updateContext();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("GETTING LOGICAL-PLAN");
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