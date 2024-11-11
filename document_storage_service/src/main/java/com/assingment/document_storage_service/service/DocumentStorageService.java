package com.assignment.document_storage_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

@Service
public class DocumentStorageService {

    @Autowired
    private S3Client s3Client;
    private static final Logger logger = LoggerFactory.getLogger(DocumentStorageService.class);

    public DocumentStorageService(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public Boolean checkIfFolderExist(String bucketName, String folderName){
        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(folderName)
                .maxKeys(1)
                .build();

        ListObjectsV2Response response = s3Client.listObjectsV2(request);
        return !response.contents().isEmpty();
    }

    public String searchFile(String bucketName, String folderName, String fileName) {
        if(Boolean.FALSE.equals(checkIfFolderExist(bucketName,folderName))){
            logger.error("Folder name {} does not exist in the bucket {}",folderName,bucketName);
            return null;
        }

        ListObjectsV2Request objectRequests = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(folderName)
                .build();

        ListObjectsV2Response objectResponses = s3Client.listObjectsV2(objectRequests);
        logger.info("object count is {}",objectResponses.keyCount());
        for(S3Object object : objectResponses.contents()){
            if(object.key().endsWith(fileName)) {
                logger.info("returning the object after searching");
                return object.key();
            }
        }
        logger.error("File {} not found in the object {}", fileName, folderName);
        return null;
    }

    public byte[] downloadFile(String bucketName, String fileName) throws IOException{

        byte[] content;
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();

        try {
            content = s3Client.getObject(getObjectRequest).readAllBytes();
            logger.info("downloaded the file: {}",fileName);
            return content;
        } catch (S3Exception e) {
            throw new NoSuchFileException("file doesn't exist");
        }
    }
}
