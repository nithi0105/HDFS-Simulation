//Written By Shaleen Garg
package ds.mapreduce;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import ds.mapreduce.maprformat.*;
import ds.hdfs.hdfsformat.*;
import com.google.protobuf.ByteString; 
import com.google.protobuf.InvalidProtocolBufferException;
import ds.hdfs.Client;
import ds.hdfs.INameNode;

public class TaskTracker
{
    public TaskTracker(int id, int mapthreads, int reducethreads)
    {
    }

    public IJobTracker GetJTStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IJobTracker stub = (IJobTracker) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                System.out.println("Still waiting for JobTracker");
                continue;
            }
        }
    }

    public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("NameNode Found");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        //Send Heartbeat to the JT
        while(true)
        {
            try{
                TimeUnit.SECONDS.sleep(1); //Wait for 1 Seconds
            }catch(Exception e){
                System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
            }
        }
    }
}

class Maptasks
{
    public Maptasks(){}
}

class Reducetasks
{
    public Reducetasks(){}
}

//This function will load the mapper function from the jar; perform it -
//And write it to a file job_<jobid>_map_<taskid>
class MapperFunc implements Callable<Integer>
{
    //This is the function which will be called everytime MapperFunc is called
    public Integer call() throws IOException, ClassNotFoundException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, InstantiationException
    {
        boolean bool = false;
        try{
        }catch(Exception e){
            System.out.println("Error");
            return -1;
        }
        return 1;
    }
}

class ReducerFunc implements Callable<Integer> 
{

    //This is the function which will be called everytime ReducerFunc is called
    public Integer call() throws FileNotFoundException, IOException
    {
        System.out.println("Going to Start ReducerFunc");
        //Add your code here
        return 1;
    }
}
