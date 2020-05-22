package main;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
 
/**
* <B>创  建 人：</B>AdministratorReyoAut <BR>
* <B>创建时间：</B>2017年12月23日 下午9:25:21<BR>
*
* @author ReYo
* @version 1.0
*/
public class ReYoFileListener implements FileAlterationListener {
 
    ReYoFileMonitor monitor = null;
    Main a = new Main();
    @Override
    public void onStart(FileAlterationObserver observer) {
        //System.out.println("onStart");
    }
 
    @Override
    public void onDirectoryCreate(File directory) {
        System.out.println("onDirectoryCreate:" + directory.getName());
    }
 
    @Override
    public void onDirectoryChange(File directory) {
        System.out.println("onDirectoryChange:" + directory.getName());
    }
 
    @Override
    public void onDirectoryDelete(File directory) {
        System.out.println("onDirectoryDelete:" + directory.getName());
        System.out.println(directory.getPath());
    }
 
    @Override
    public void onFileCreate(File file) {
        System.out.println("onFileCreate:" + file.getName());
        if(file.length()>20971520)
		 {
			 Main.upload_large(file.getPath(),Main.getBucketName());
			 }
		 else {
			 Main.upload_small(file.getPath(),Main.getBucketName());
		 }
        System.out.println("FileCreate have done");
    }
 
    @Override
    public void onFileChange(File file) {
        System.out.println("onFileChange : " + file.getName());
        if(file.length()>20971520)
		 {
			 Main.upload_large(file.getPath(),Main.getBucketName());
			 }
		 else {
			 Main.upload_small(file.getPath(),Main.getBucketName());
		 }
    }
 
    @Override
    public void onFileDelete(File file) {
//    	if(!file.getName())
        System.out.println("onFileDelete :" + file.getName());
        
        a.deleteFile(file.getName());
    
    }
 
    @Override
    public void onStop(FileAlterationObserver observer) {
        //System.out.println("onStop");
    }
 
}
