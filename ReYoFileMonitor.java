package main;

import java.io.File;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
 
/**
* <B>��  �� �ˣ�</B>AdministratorReyoAut <BR>
* <B>����ʱ�䣺</B>2017��12��23�� ����9:26:08<BR>
*
* @author ReYo
* @version 1.0
*/
public class ReYoFileMonitor {
 
    FileAlterationMonitor monitor = null;
 
    public ReYoFileMonitor(long interval) throws Exception {
        monitor = new FileAlterationMonitor(interval);
    }
 
    public void monitor(String path, FileAlterationListener listener) {
        FileAlterationObserver observer = new FileAlterationObserver(new File(path));
        monitor.addObserver(observer);
        observer.addListener(listener);
    }
 
    public void stop() throws Exception {
        monitor.stop();
    }
 
    public void start() throws Exception {
        monitor.start();
    }
 
    public static void main(String[] args) throws Exception {
       
    }

	
}