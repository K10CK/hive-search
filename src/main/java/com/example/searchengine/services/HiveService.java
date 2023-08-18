package com.example.searchengine.services;

import com.example.searchengine.repositories.HiveRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class HiveService {

    @Autowired
    private HiveRepository hiveRepository;

//    public List<Map<String, Object>> getTables(String schema) {
//        return hiveRepository.getTables(schema);
//    }
//
//    public List<Map<String, Object>> getSchemas() {
//        return hiveRepository.getSchemas();
//    }
//
//    public List<Map<String, Object>> getTablePreview(String schema, String table) {
//        return hiveRepository.getTablePreview(schema, table);
//    }

    public List<Map<String, Object>> getSearch(String schema, String table, String search) {
        return hiveRepository.getSearch(schema, table, search);
    }

    private final JdbcTemplate jdbcTemplate;

    public HiveService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<String> getHeaders(InputStream inputStream) throws Exception {
        List<String> headers = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = reader.readLine();
            if (line != null) {
                String[] headerArray = line.split("\",\"");
                for (String header : headerArray) {
                    headers.add(formatHeaders(header.trim()));
                }
            }
        }

        return headers;
    }

    private String formatHeaders(String header) {  //Format Headers so they have no special characters and can be accepted by hive when creating table (converts data types of all fields to string)
        String formatHeader = header.replaceAll("[^a-zA-Z0-9_]", "").replaceAll(" ", "_");
        return formatHeader + " string";
    }


    public void dropTable(String schema, String table) {
        jdbcTemplate.execute("use " + schema);
        String dropQuery = "DROP TABLE IF EXISTS " + table;
        jdbcTemplate.execute(dropQuery);
    }

    public void createTable(String schema, String table, List<String> headers) {
        jdbcTemplate.execute("use " + schema);
        String createQuery = "CREATE TABLE " + table + " (" +
                String.join(", ", headers) +
                ") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'";
        jdbcTemplate.execute(createQuery);
    }


    private final String hdfsUri = "hdfs://localhost:9000";

    public void hdfsUpload(MultipartFile file, String targetPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);

        try (FileSystem fs = FileSystem.get(conf)) {
            Path hdfsTargetPath = new Path(targetPath, file.getOriginalFilename());
            try (OutputStream outputStream = fs.create(hdfsTargetPath)) {
                outputStream.write(file.getBytes());
            }
        } catch (IOException e) {
            throw e;
        }
    }

    public void loadData(String schema, String table, String hdfsFilePath) {
        jdbcTemplate.execute("use " + schema);
        String loadQuery = "LOAD DATA INPATH '" + hdfsFilePath + "' INTO TABLE " + table;

        jdbcTemplate.execute(loadQuery);
    }


    public List<Map<String, Object>> search(String schema, String table, String header, String search) {
        jdbcTemplate.execute("use " + schema);
        String query = "SELECT * FROM "+ table + " WHERE " + header + " LIKE '%" + search + "%'";
        return jdbcTemplate.queryForList(query);
    }

    public void stagingTable(String schema, String stagingTable, List<String> headers) {
        jdbcTemplate.execute("use " + schema);
        String createQuery = "CREATE TABLE " + stagingTable + " (" +
                String.join(", ", headers) +
                ") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'";
        jdbcTemplate.execute(createQuery);
    }

    public void insertIntoORC(String schema, String stagingTable, String orcTable) {
        jdbcTemplate.execute("use " + schema);

        // Insert data from staging table into ORC table
        String insertQuery = "INSERT INTO TABLE " + orcTable +
                " SELECT * FROM " + stagingTable;
        jdbcTemplate.execute(insertQuery);
    }
}
