package com.example.searchengine.controllers;

import com.example.searchengine.services.HiveService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/{schema}/{table}")
public class FileUploadController {

    private static final Logger logger = LoggerFactory.getLogger(FileUploadController.class);

    private final HiveService hiveService;

    public FileUploadController(HiveService hiveService) {
        this.hiveService = hiveService;
    }

    private List<String> formattedHeaders1;  //Variable outside uploadCSVFile so it can be called at "/headers"

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@PathVariable String schema,
                                             @PathVariable String table,
                                             @RequestParam("file") MultipartFile file,
                                             RedirectAttributes redirectAttributes) {

        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("Please select a file to upload.");
        }

        try {
            String stagingTable = table + "_staging";

            hiveService.dropTable(schema, stagingTable);
            hiveService.dropTable(schema, table);

            List<String> formattedHeaders = hiveService.getHeaders(file.getInputStream());
            formattedHeaders1 = formattedHeaders;

            hiveService.stagingTable(schema, stagingTable, formattedHeaders);
            hiveService.createTable(schema, table, formattedHeaders);

            String hdfsDirectory = "/user/hive/dataset";
            String hdfsFilePath = hdfsDirectory + "/" + file.getOriginalFilename();

            hiveService.hdfsUpload(file, hdfsDirectory);

            logger.info("File uploaded to hdfs");

            hiveService.loadData(schema, stagingTable, hdfsFilePath);

            logger.info("File loaded into staging");

            hiveService.insertIntoORC(schema, stagingTable, table);

            logger.info("File uploaded and table created successfully.");

            return ResponseEntity.ok("File uploaded and table created successfully.");
        } catch (Exception e) {
            logger.error("Error uploading file and creating table: " + e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error uploading file and creating table: " + e.getMessage());
        }
    }
    @GetMapping("/headers")
    @ResponseBody
    public List<String> getHeaders() {
        List<String> cleanedHeaders = new ArrayList<>();

        for (String header : formattedHeaders1) {
            String cleanedHeader = header.replace(" string", ""); // Remove " string" suffix
            cleanedHeaders.add(cleanedHeader);
        }
        return cleanedHeaders;
    }

}





