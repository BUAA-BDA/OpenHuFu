package com.hufudb.openhufu.owner.minio;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.*;
import io.minio.messages.Bucket;

import java.io.*;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class MinioClientUtil {

    private final MinioClient minioClient;
    public MinioClientUtil() {
        String httpUrl = System.getenv("minioUrl");
        String accessKey = System.getenv("minioAccess");
        String secretKey = System.getenv("minioSecret");
        minioClient = MinioClient.builder().endpoint(httpUrl).credentials(accessKey, secretKey).build();
    }

    public void uploadFile(String bucketName, String objectName, File file) {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(objectName).stream(inputStream, inputStream.available(), -1).build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        try {
            String httpUrl = "http://192.168.40.230:32000";
            String accessKey = "minio";
            String secretKey = "minio123";
            MinioClient minioClient = MinioClient.builder().endpoint(httpUrl).credentials(accessKey, secretKey).build();
            for (Bucket bucket : minioClient.listBuckets()) {
                System.out.println(bucket.name() + " " + bucket.creationDate());
            }
            String bucketName = "result";
            String dirName = "dirName";
            String fileName = "fileName";
            File file = new File("1.txt");
            System.out.println("__________________");

        } catch (ServerException e) {
            throw new RuntimeException(e);
        } catch (InsufficientDataException e) {
            throw new RuntimeException(e);
        } catch (ErrorResponseException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        } catch (InvalidResponseException e) {
            throw new RuntimeException(e);
        } catch (XmlParserException e) {
            throw new RuntimeException(e);
        } catch (InternalException e) {
            throw new RuntimeException(e);
        }
    }
}
