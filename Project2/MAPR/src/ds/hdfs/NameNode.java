package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
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

import ds.hdfs.HdfsDefn.DataNode;
import ds.hdfs.HdfsDefn.Block;

import com.google.protobuf.*;

//import ds.hdfs.hdfsformat.*;

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
		HdfsDefn.File.Builder ret = HdfsDefn.File.newBuilder();
		try
		{
			HdfsDefn.File f = HdfsDefn.File.parseFrom(inp);
			//File file = new File("./tmp/" + f.getName());
			
	        FileInputStream fileInputStream = new FileInputStream(f.getName());
	        FileDescriptor fd = fileInputStream.getFD();
	        System.out.println("fd: " + fd.hashCode());
	        
	        ret.setName(f.getName());
	        ret.setHandle(fd.hashCode());
	        ret.setWritemode(f.getWritemode());
	        ret.addAllChunks(f.getChunksList());
	        
	        fileInputStream.close();
		}
		catch (Exception e) 
		{
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		return ret.build().toByteArray();
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
			//get filename.txt
			//traverse array
			//blockFile = filename.whtever + .[number at index i]
			//new File("./tmp/"+ blockFile);
			HdfsDefn.File f = HdfsDefn.File.parseFrom(inp);
			File folder = new File("./tmp");
			File[] listOfFiles = folder.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				//blockFile = filename.whtever + .[number at index i]
				if (listOfFiles[i].getName().equals("")) {
					System.out.println("File " + listOfFiles[i].getName());
				}
			}
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
		HdfsDefn.File.Builder retFile = HdfsDefn.File.newBuilder();
		
		try
		{
			HdfsDefn.File f = HdfsDefn.File.parseFrom(inp);
			retFile.setName(f.getName());
			//find blocks to put together file for read
			
	        //file size/block size = # of blocks to a file -- replication factor is 2
			long fileSize = new File(f.getName()).length();
			long blockSize = 64; //make configurable (read config)
			long numBlocks = (long) Math.ceil(fileSize/blockSize);
			for(int i = 0; i < numBlocks; i++) {
				Block.Builder chunk = Block.newBuilder();
				HdfsDefn.DataNode.Builder dn = HdfsDefn.DataNode.newBuilder();
				
				String blockName = f.getName() + "." + i;
				chunk.setName(blockName);
				
				/*String dnName = ipaddress  (read config)
				dn.setId(dnName);
				dn.setStatus(HdfsDefn.DataNode.Status.ALIVE);
				chunk.addDatanodes(dn);*/
				
				retFile.addChunks(chunk);
			}
			
			HdfsDefn.File result = retFile.build();
			FileOutputStream output = new FileOutputStream("result_protobuf");
			result.writeTo(output);
			
			//read file -> filename: block list
			//parsefrom protobuf file
			//getblocklocations(block list) ?
			
			
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
			//list all files in arraylist (no directory implementation)
			//persist array list of files and chunks into "dn_output.txt" (did so in heartbeat)
			//parse proto file
			try {
				HdfsDefn.Result_File result = HdfsDefn.Result_File.parseFrom(new FileInputStream("result_protobuf"));
				for(HdfsDefn.File file : result.getFileList()) {
					System.out.println(file.getName());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("Error at list "+ e.toString());
				e.printStackTrace();
				response.setStatus(HdfsDefn.DataNode.Status.DEAD);
			}
			
		return response.build().toByteArray();
	}
	
	// Datanode <-> Namenode interaction methods
		
	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		PrintWriter writer = null;
        //write to file "filename: filename.1, filename.2, filename.3, ..."
        //file to protobuf
		
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter("result_output.txt", true)));
			writer.print(response.getId() + ":" + response.getTimestamp() + ":");

			HdfsDefn.File result = HdfsDefn.File.parseFrom(inp);
			//HdfsDefn.File result = HdfsDefn.File.parseFrom(new FileInputStream("result_protobuf"));
			writer.print(result.getName() + ":");
			int dnCounter = 0;
		    
			for(HdfsDefn.Block block : result.getChunksList()) {
				writer.print("[" + block.getName() + ":");
				int numDn = block.getDatanodesCount();
				
				for(HdfsDefn.DataNode dn : block.getDatanodesList()) {
					dnCounter++;
					if(dnCounter == numDn) {
						writer.print(dn.getId() + "," + dn.getStatus() + ":");
					} else {
						writer.print(dn.getId() + "," + dn.getStatus() + "]");
					}
				}
			}
		    writer.println("");

		} catch (IOException e) {
			System.err.println("Error at blockReport "+ e.toString());
			e.printStackTrace();
			response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		} finally {
	        if (writer != null) {
	            writer.close();
	        }
	        else {
	            //System.out.println("PrintWriter not open");
	        }
		}
		return response.build().toByteArray();
	}
	
	
	
	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		try {
			HdfsDefn.DataNode dd = HdfsDefn.DataNode.parseFrom(inp);
			response.setId(dd.getId());
			response.setReplicas(dd.getReplicas());
			//response.addAllBlocks(dd.getBlocksList());
			response.setStatus(HdfsDefn.DataNode.Status.ALIVE);
			response.setTimestamp(dd.getTimestamp());
			
			
			
		switch(dd.getStatus()) {
				case ALIVE:
					response.setStatus(HdfsDefn.DataNode.Status.ALIVE);
					//update file check if datanode is in the file
					break;
				case DEAD:
					response.setStatus(HdfsDefn.DataNode.Status.DEAD);
					//delete blocks in block list
					//update file with delete
					break;
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
		
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
            registry.bind("INameNode", stub);

            System.err.println("Server ready");
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
        
        //write a file in hdfs
        NameNode.FileInfo file = new NameNode.FileInfo("file", 1, true);
        file.Chunks = new ArrayList<Integer>();

        
	}
	
}