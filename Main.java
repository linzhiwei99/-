package main;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
public class Main {
	private final static String accessKey = "DE6EEA2A384A7A79314D";
	private final static String secretKey = "WzhDMEIyMjlDRURFOUYwNDRBQ0ZGMEJGQTczMzkyN0VDQzEwNkVFRkRd";
	private final static String serviceEndpoint = 
			"http://scuts3.depts.bingosoft.net:29999";
	private static long partSize = 5 << 20;
	private final static String signingRegion = "";
	public static void main(String[] args) {
		//第一次先对文件进行同步
		
			//提醒输入目录名和Bucketname
//			System.out.print("请输入目录名和Bucket_name:");
//			System.out.print("目录名:");
			//输入目录名和Bucketname
			//Scanner sc = new Scanner(System.in);
			//String file_name = sc.nextLine();
			String file_name="D:\\file_test\\";
//			System.out.print("Bucket_name:");
			String Bucket_name="linzhiwei";
			//String Bucket_name = sc.nextLine();
			//对目录名字符串处理
			
			//上传文件
			ArrayList<String> listFileName = new ArrayList<String>(); 
			getAllFileName(file_name,listFileName);
			 for(String name:listFileName){
				 System.out.println(name);
				 final File file = new File(name);
				 System.out.println(file.length());
				 if(file.length()>50000)
				 {
					 upload_large(name,Bucket_name);
					 }
				 else {
					 upload_small(name,Bucket_name);
				 }
				 }
			 }
		
		
		
		
		//文件监听器，根据不同的变化调用不同的函数
	
	//获取目录下的所有文件的绝对路径的函数
	public static void getAllFileName(String path,ArrayList<String> listFileName) {
		 File file = new File(path);
	     File [] files = file.listFiles();
	     String [] names = file.list();
	     if(names != null){
	            String [] completNames = new String[names.length];
	            for(int i=0;i<names.length;i++){
	            	completNames[i]=path+names[i];
	            }
	            listFileName.addAll(Arrays.asList(completNames));
	}
	     for(File a:files){
	            if(a.isDirectory()){//如果文件夹下有子文件夹，获取子文件夹下的所有文件全路径
	            	 getAllFileName(a.getAbsolutePath()+"\\",listFileName);
	            }
	     }
	}	
	

	
	//传输20M以上文件到S3的函数
	private static void upload_large(String filePath,String bucketName ) {
		final BasicAWSCredentials credentials = 
				new BasicAWSCredentials(accessKey,secretKey);
				final ClientConfiguration ccfg = new ClientConfiguration().
						withUseExpectContinue(true);

				final EndpointConfiguration endpoint = 
		new EndpointConfiguration(serviceEndpoint, signingRegion);

				final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
		                .withCredentials(new AWSStaticCredentialsProvider(credentials))
		                .withClientConfiguration(ccfg)
		                .withEndpointConfiguration(endpoint)
		                .withPathStyleAccessEnabled(true)
		                .build();
				
				String keyName = Paths.get(filePath).getFileName().toString();
				System.out.println(keyName);
				// Create a list of UploadPartResponse objects. You get one of these
		        // for each part upload.
				ArrayList<PartETag> partETags = new ArrayList<PartETag>();
				File file = new File(filePath);
				long contentLength = file.length();
					String uploadId = null;
					try {
						
						// Step 1: Initialize.
						InitiateMultipartUploadRequest initRequest = 
								new InitiateMultipartUploadRequest(bucketName, keyName);
						uploadId = s3.initiateMultipartUpload(initRequest).getUploadId();
						System.out.format("Created upload ID was %s\n", uploadId);

						// Step 2: Upload parts.	
						long filePosition = 0;
						for (int i = 1; filePosition < contentLength; i++) {
							// Last part can be less than 5 MB. Adjust part size.
							partSize = Math.min(partSize, contentLength - filePosition);

							// Create request to upload a part.
							UploadPartRequest uploadRequest = new UploadPartRequest()
									.withBucketName(bucketName)
									.withKey(keyName)
									.withUploadId(uploadId)
									.withPartNumber(i)
									.withFileOffset(filePosition)
									.withFile(file)
									.withPartSize(partSize);

							// Upload part and add response to our list.
							System.out.format("Uploading part %d\n", i);
							partETags.add(s3.uploadPart(uploadRequest).getPartETag());
							filePosition += partSize;
						}

						// Step 3: Complete.
						System.out.println("Completing upload");
						CompleteMultipartUploadRequest compRequest = 
								new CompleteMultipartUploadRequest(bucketName, keyName, uploadId, partETags);

						s3.completeMultipartUpload(compRequest);
					} catch (Exception e) {
						System.err.println(e.toString());
						if (uploadId != null && !uploadId.isEmpty()) {
							// Cancel when error occurred
							System.out.println("Aborting upload");
							s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, keyName, uploadId));
						}
						System.exit(1);
					}
					System.out.println("Done!");
			// TODO Auto-generated method stub
				}
	
	
	
	//	传输20M以下的文件函数
	public static void upload_small(String filePath,String bucketName ){
        final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final ClientConfiguration ccfg = new ClientConfiguration().
                withUseExpectContinue(false);

        final EndpointConfiguration endpoint = new EndpointConfiguration(serviceEndpoint, signingRegion);

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(ccfg)
                .withEndpointConfiguration(endpoint)
                .withPathStyleAccessEnabled(true)
                .build();

        System.out.format("Uploading %s to S3 bucket %s...\n", filePath, bucketName);
        final String keyName = Paths.get(filePath).getFileName().toString();
        final File file = new File(filePath);

        for (int i = 0; i < 2; i++) {
            try {
                s3.putObject(bucketName, keyName, file);
                break;
            } catch (AmazonServiceException e) {
                if (e.getErrorCode().equalsIgnoreCase("NoSuchBucket")) {
                    s3.createBucket(bucketName);
                    continue;
                }

                System.err.println(e.toString());
                System.exit(1);
            } catch (AmazonClientException e) {
                try {
                    // detect bucket whether exists
                    s3.getBucketAcl(bucketName);
                } catch (AmazonServiceException ase) {
                    if (ase.getErrorCode().equalsIgnoreCase("NoSuchBucket")) {
                        s3.createBucket(bucketName);
                        continue;
                    }
                } catch (Exception ignore) {
                }

                System.err.println(e.toString());
                System.exit(1);
            }
        }

        System.out.println("Done!");
    }
	}
	
	
	

	
