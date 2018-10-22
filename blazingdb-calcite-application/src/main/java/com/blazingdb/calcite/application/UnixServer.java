/*
 * This file is part of the JNR project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blazingdb.calcite.application;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.calcite.plan.RelOptUtil;

import com.blazingdb.calcite.catalog.connection.CatalogService;
import com.blazingdb.calcite.catalog.connection.CatalogServiceImpl;
import com.blazingdb.calcite.schema.BlazingSchema;
import com.blazingdb.protocol.IService;
import com.blazingdb.protocol.UnixService;
import com.blazingdb.protocol.message.RequestMessage;
import com.blazingdb.protocol.message.ResponseErrorMessage;
import com.blazingdb.protocol.message.ResponseMessage;
import com.blazingdb.protocol.message.calcite.DDLCreateTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLDropTableRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLRequestMessage;
import com.blazingdb.protocol.message.calcite.DDLResponseMessage;
import com.blazingdb.protocol.message.calcite.DMLRequestMessage;
import com.blazingdb.protocol.message.calcite.DMLResponseMessage;

import blazingdb.protocol.Status;
import blazingdb.protocol.calcite.MessageType;
import com.google.gson.*;

 public class UnixServer {

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
	public static void main(String[] args) throws IOException {
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

        DDLCreateTableRequestMessage message = new DDLCreateTableRequestMessage(columnNames, types, name, dbName);
        try {
            ApplicationContext.getCatalogService().createTable(message);
            ApplicationContext.updateContext();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String query = params.query;
        DMLRequestMessage requestPayload = new DMLRequestMessage(query);
        try {
            String logicalPlan  = RelOptUtil.toString(ApplicationContext.getRelationalAlgebraGenerator().getRelationalAlgebra(requestPayload.getQuery()));
            System.out.println("@JSON");
            System.out.println(logicalPlan);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
