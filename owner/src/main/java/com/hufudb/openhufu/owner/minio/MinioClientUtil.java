package com.hufudb.openhufu.owner.minio;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.errors.*;
import io.minio.messages.Bucket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class MinioClientUtil {
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
            InputStream inputStream = new FileInputStream(file);
            minioClient.putObject(PutObjectArgs.builder().bucket(bucketName).object(dirName+"/"+fileName).stream(inputStream, inputStream.available(), -1).build());
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
