package com.assignment.document_storage_service.controller;

import com.assignment.document_storage_service.service.DocumentStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;

@RestController
public class DocumentStorageController {

    @Autowired
    private DocumentStorageService documentStorageService;

    private static final Logger logger = LoggerFactory.getLogger(DocumentStorageController.class);

    @GetMapping("/download")
    public ResponseEntity<?> downloadFile(
            @RequestParam String bucketName,
            @RequestParam String folderName,
            @RequestParam String fileName
    )

    {

        try{
            String filePath = documentStorageService.searchFile(bucketName,folderName,fileName);

            if(!filePath.isEmpty()){

                logger.info("started downloading the file: {}", filePath);
                byte[] data = documentStorageService.downloadFile(bucketName, filePath);
                ByteArrayResource resource = new ByteArrayResource(data);
                logger.info("downloaded successfully");
                return ResponseEntity.status(HttpStatus.OK)
                        .header(HttpHeaders.CONTENT_TYPE, "application/octet-stream")
                        .header(HttpHeaders.CONTENT_DISPOSITION, ContentDisposition.attachment().filename(fileName).build().toString())
                        .body(resource);
            }
            else {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("File doesn't exist");
            }
        }
        catch (S3Exception exception){
            logger.error("error occurred: {}",exception.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Error " + exception.getMessage());
        }
        catch (IOException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }
}
