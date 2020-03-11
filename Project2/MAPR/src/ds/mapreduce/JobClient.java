//Written By Shaleen Garg
package ds.mapreduce;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;
//import ds.hdfs.hdfsformat.*;

import ds.mapreduce.maprformat.*;
import com.google.protobuf.ByteString; 
import ds.hdfs.Client;

public class JobClient
{
    public JobClient(String mapname, String reducername, String inputfile, String outputfile, int numreducers)
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

    public static void main(String[] args)
    {
        if(args.length != 5) //Check if it was called correctly
        {
            System.out.println("invoke in format $java ds.mapreduce.JobClient <mapName> <reducerName> <InputFile> <OutputFile> <numReducers>");
            return ;
        }
        JobClient JC = new JobClient(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]));
        Client HC = new Client();
        //
        //Submitting a Job
        boolean JobDone = false;
        while(JobDone == false)
        {
            try{
            }catch(Exception e){
                System.out.println("Remote Error while getting JobStatus response");
                return;
            }

            try{
                TimeUnit.SECONDS.sleep(2); //Wait for 1 Seconds
            }catch(Exception e){
                System.out.println("Unexpected Interrupt Exception while waiting for BlockReport");
            }

        }
        System.out.println("MapReduce Job Complete !!!");
    }
}
