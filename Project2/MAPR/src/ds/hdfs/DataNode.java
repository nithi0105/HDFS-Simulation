//Written By Shaleen Garg
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.nio.charset.Charset;

//import ds.hdfs.hdfsformat.*;
import ds.hdfs.IDataNode.*;

public class DataNode implements IDataNode
{
	HdfsDefn.DataNode.Builder response = HdfsDefn.DataNode.newBuilder(); //temp
    protected String MyChunksFile;
    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
    protected String MyName;
    protected int MyID;

    public DataNode(String name, int storagePort, String IP)
    {
        //Constructor
    	this.MyName = name;
    	this.MyPort = storagePort;
        this.MyIP = IP;
    }

    /*public static void appendtoFile(String Filename, String Line)
    {
        BufferedWriter bw = null;

        try {
            //append
        } 
        catch (IOException ioe) 
        {
            ioe.printStackTrace();
        } 
        finally 
        {                       // always close the file
            if (bw != null) try {
                bw.close();
            } catch (IOException ioe2) {
            }
        }

    }*/

    public byte[] readBlock(byte[] Inp)
    {
        try
        {
        }
        catch(Exception e)
        {
            System.out.println("Error at readBlock");
            //response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    public byte[] writeBlock(byte[] Inp)
    {
        try
        {
        }
        catch(Exception e)
        {
            System.out.println("Error at writeBlock ");
            //response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    public void BlockReport() throws IOException
    {
    }

    public void BindServer(String Name, String IP, int Port)
    {
        try
        {
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
            System.setProperty("java.rmi.server.hostname", IP);
            Registry registry = LocateRegistry.createRegistry(Port);
            registry.rebind(Name, stub);
            System.out.println("\nDataNode connected to RMIregistry\n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
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
                System.out.println("NameNode Found!");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }

    public static void main(String args[]) throws InvalidProtocolBufferException, IOException
    {
        //Define a Datanode Me
        DataNode Me = new DataNode("cp", 2005, "128.6.13.177"); //get from dn config?
        INameNode stub = Me.GetNNStub("INameNode", "128.6.13.175", 2007); //get from nn config?
        Me.NNStub = stub;
        Me.BindServer("IDataNode", "128.6.13.177", 2005); //get from config

    }
}