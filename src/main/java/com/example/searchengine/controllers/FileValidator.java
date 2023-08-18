package com.example.searchengine.controllers;

import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.web.multipart.MultipartFile;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

@Component
public class FileValidator implements Validator {

    @Override
    public boolean supports(Class<?> clazz) {
        return MultipartFile.class.isAssignableFrom(clazz);
    }

    @Override
    public void validate(Object target, Errors errors) {
        MultipartFile file = (MultipartFile) target;

        if (file.isEmpty()) {
            errors.rejectValue("file", "file.empty", "File must not be empty");
        } else if (!file.getOriginalFilename().endsWith(".csv")) {
            errors.rejectValue("file", "file.invalidType", "Invalid file type. Only CSV files are allowed.");
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            String headerLine = reader.readLine(); // Assuming first line is header
            if (headerLine == null) {
                errors.rejectValue("file", "file.emptyContent", "CSV file is empty.");
            } else {
                String[] headerColumns = headerLine.split(",");
                if (headerColumns.length < 2) {
                    errors.rejectValue("file", "file.invalidHeader", "CSV file must have at least 2 columns.");
                }
            }


        } catch (IOException e) {
            errors.rejectValue("file", "file.readError", "Error reading the uploaded file");
        }
    }
}