package com.assignment.document_storage_service.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DocumentStorageServiceTest {

    private S3Client mockedS3Client;
    private DocumentStorageService documentStorageService;

    @BeforeEach
    void setup(){
        MockitoAnnotations.openMocks(this);
        mockedS3Client = mock(S3Client.class);
        documentStorageService = new DocumentStorageService(mockedS3Client);
    }

    @Test
    void testCheckFolderIfFolderExist(){

        ListObjectsV2Response mockResponse = ListObjectsV2Response.builder().contents(S3Object.builder().key("folderName/").build()).build();
        when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);
        boolean result = documentStorageService.checkIfFolderExist("bucket-name","folderName");
        Assertions.assertTrue(result);
    }

    @Test
    void testCheckFolderIfFolderDoesNotExist(){

        ListObjectsV2Response mockResponse = ListObjectsV2Response.builder().contents(Collections.emptyList()).build();
        when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);
        boolean result = documentStorageService.checkIfFolderExist("bucket-name","folderName");
        Assertions.assertFalse(result);
    }

    @Test
    void testSearchFileIfFileExist(){

        String bucketName = "bucket-name";
        String folderName = "folder-name";
        String fileName = "file-name";

        S3Object s3Object = S3Object.builder()
                .key(folderName+"/"+fileName)
                .build();
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
                .contents(List.of(s3Object))
                .build();

        Mockito.when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listResponse);
        String result = documentStorageService.searchFile(bucketName,folderName,fileName);

        Assertions.assertEquals(folderName+"/"+fileName,result);
    }

    @Test
    void testSearchFileIfFolderDoesNotExist(){

        String bucketName = "bucket-name";
        String folderName = "folder-name1";
        String fileName = "file-name";

        DocumentStorageService spyDocument = spy(documentStorageService);

        doReturn(false).when(spyDocument).checkIfFolderExist(bucketName,folderName);
        ListObjectsV2Response mockResponse = ListObjectsV2Response.builder().contents(Collections.emptyList()).build();
        when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(mockResponse);
        String result = documentStorageService.searchFile(bucketName,folderName,fileName);

        Assertions.assertNull(result);
    }

    @Test
    void testSearchFileIfFileDoesNotExist(){

        String bucketName = "bucket-name";
        String folderName = "folder-name";
        String fileName = "file-name";

        S3Object s3Object = S3Object.builder()
                .key(folderName+"/"+"change_in_file_name")
                .build();
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
                .contents(List.of(s3Object))
                .build();

        Mockito.when(mockedS3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listResponse);
        String result = documentStorageService.searchFile(bucketName,folderName,fileName);

        Assertions.assertNull(result);
    }

    @Test
    void testDownloadFileIfSuccess() throws IOException {

        String bucketName = "bucket-name";
        String fileName = "folder-name";
        byte[] expected = "file".getBytes();

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();

        ResponseInputStream<GetObjectResponse> response = new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(expected));
        when(mockedS3Client.getObject(getObjectRequest)).thenReturn(response);
        byte[] actual = documentStorageService.downloadFile(bucketName,fileName);

        Assertions.assertArrayEquals(expected,actual);
    }

    @Test
    void testDownloadFileIfFailed() {

        String bucketName = "bucket-name";
        String fileName = "folder-name";

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();

        when(mockedS3Client.getObject(getObjectRequest)).thenThrow(S3Exception.builder().message("file doesn't exist").build());
        Assertions.assertThrows(NoSuchFileException.class, ()-> {documentStorageService.downloadFile(bucketName,fileName);});
    }
}
