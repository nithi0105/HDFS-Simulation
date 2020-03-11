package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.*;

//import ds.hdfs.hdfsformat.*;
import ds.hdfs.HdfsDefn.*;

public class NameNode implements INameNode{

	protected Registry serverRegistry;
	HdfsDefn.DataNode.Builder response = HdfsDefn.DataNode.newBuilder();
	String ip;
	int port;
	String name;

	public NameNode(String addr,int p, String nn)
	{
		ip = addr;
		port = p;
		name = nn;
	}

	public static class DataNode
	{
		String ip;
		int port;
		String serverName;
		public DataNode(String addr,int p,String sname)
		{
			ip = addr;
			port = p;
			serverName = sname;
		}
	}

	public static class FileInfo
	{
		String filename;
		int filehandle;
		boolean writemode;
		ArrayList<Integer> Chunks;
		public FileInfo(String name, int handle, boolean option)
		{
			filename = name;
			filehandle = handle;
			writemode = option;
			Chunks = new ArrayList<Integer>();
		}
	}
	/* Method to open a file given file name with read-write flag*/

	boolean findInFilelist(int fhandle)
	{
		return false;
	}

	public void printFilelist()
	{
	}

	public byte[] openFile(byte[] inp) throws RemoteException
	{
		try
		{
		}
		catch (Exception e)
		{
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		return response.build().toByteArray();
	}

	public byte[] closeFile(byte[] inp ) throws RemoteException
	{
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at closefileRequest " + e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}

		return response.build().toByteArray();
	}

	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at getBlockLocations "+ e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		return response.build().toByteArray();
	}


	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		try
		{
			//2 blocks per datanode
		}
		catch(Exception e)
		{
			System.err.println("Error at AssignBlock "+ e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}

		return response.build().toByteArray();
	}


	public byte[] list(byte[] inp ) throws RemoteException
	{
		try
		{
		}catch(Exception e)
		{
			System.err.println("Error at list "+ e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		return response.build().toByteArray();
	}

	// Datanode <-> Namenode interaction methods

	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at blockReport "+ e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		return response.build().toByteArray();
	}



	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		return response.build().toByteArray();
	}

	public void printMsg(String msg)
	{
		System.out.println(msg);
	}

	public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException
	{
        try {
            NameNode obj = new NameNode(null, 0, null);
            INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("Hello", stub);

            System.err.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
	}

}
