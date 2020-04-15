package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RemoteServer;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;

import ds.hdfs.HdfsDefn.DataNode;
import ds.hdfs.HdfsDefn.Block;

import com.google.protobuf.*;

//import ds.hdfs.hdfsformat.*;

public class NameNode implements INameNode{

	protected static Registry serverRegistry;
	//HdfsDefn.DataNode.Builder response = HdfsDefn.DataNode.newBuilder();
	boolean isExecuted = false;
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
		System.out.println("NameNode openFile command accepted");
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
	        //ret.setWritemode(f.getWritemode());
	        //ret.addAllChunks(f.getChunksList());
	        
	        fileInputStream.close();
		}
		catch (Exception e) 
		{
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			//response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		return ret.build().toByteArray();
	}
	
	/*public byte[] closeFile(byte[] inp ) throws RemoteException
	{
		try
		{
		}
		catch(Exception e)
		{
			System.err.println("Error at closefileRequest " + e.toString());
			e.printStackTrace();
			//response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		
		return response.build().toByteArray();
	}*/
	
	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		System.out.println("NameNode getting block locations");
		//find blocks to put together file for read
		HdfsDefn.Result_DataNode.Builder resDn = HdfsDefn.Result_DataNode.newBuilder();
		try
		{
			HdfsDefn.File f = HdfsDefn.File.parseFrom(inp);
			HdfsDefn.Result_File resFile = HdfsDefn.Result_File.parseFrom(new FileInputStream("file_protobuf"));
			
			for(HdfsDefn.File file : resFile.getFileList()) {
				if(file.getName().equals(f.getName())) {
					for(HdfsDefn.Block block : file.getChunksList()) {
						if(block.getName().equals(f.getName())) {
							ArrayList<HdfsDefn.DataNode> dnList = (ArrayList<HdfsDefn.DataNode>) block.getDatanodesList();
							resDn.addAllDatanode(dnList);
						}
					}
				}
			}
		}
		catch(Exception e)
		{
			System.err.println("Error at getBlockLocations "+ e.toString());
			e.printStackTrace();
			//response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}		
		return resDn.build().toByteArray();
	}
	
	
	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		System.out.println("NameNode assigning blocks");
		HdfsDefn.File.Builder retFile = HdfsDefn.File.newBuilder();
		
		try
		{
			HdfsDefn.File f = HdfsDefn.File.parseFrom(inp);
			HdfsDefn.Result_DataNode fileDn = HdfsDefn.Result_DataNode.parseFrom(new FileInputStream("dn_protobuf"));
			retFile.setName(f.getName());
			
	        //file size/block size = # of blocks to a file -- replication factor is 2
			int replicaFactor = getValuefromConfig("factor");
			int replicaCount = 0;
			long fileSize = new File(f.getName()).length();
			long blockSize = Long.valueOf(getValuefromConfig("blockBytes")); //make configurable (read config)
			long numBlocks = (long) Math.ceil(fileSize/blockSize);
			ArrayList<HdfsDefn.DataNode> dnList = (ArrayList<HdfsDefn.DataNode>) fileDn.getDatanodeList();
			Random randomizer = new Random();
			
			for(int i = 0; i < numBlocks; i++) {
				Block.Builder chunk = Block.newBuilder();
				
				String blockName = f.getName() + "." + i;
				chunk.setName(blockName);
				
				//For each block, ask the NN for a list of DNs where you will replicate them
				while(replicaCount < replicaFactor) {
					HdfsDefn.DataNode randomDn = dnList.get(randomizer.nextInt(dnList.size()));
					//look for a datanode that is alive
					while(randomDn.getStatus() == HdfsDefn.DataNode.Status.DEAD) {
						//check if this datanode already has this block
						randomDn = dnList.get(randomizer.nextInt(dnList.size()));
					}
					//can write block to this datanode
					chunk.addDatanodes(randomDn);
					replicaCount++;
				}
				replicaCount = 0;
						
				retFile.addChunks(chunk);
			}
			
			//add file to proto file
			/*HdfsDefn.File result = retFile.build();
			try {
				FileOutputStream output = new FileOutputStream("file_protobuf", true);
				result.writeTo(output);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}*/
			
		}
		catch(Exception e)
		{
			System.err.println("Error at AssignBlock "+ e.toString());
			e.printStackTrace();
			//response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		
		return retFile.build().toByteArray();
	}
		
	
	public byte[] list() throws RemoteException
	{
		System.out.println("NameNode list method is accepted");
		//list all files in arraylist (no directory implementation)
		HdfsDefn.Result_File result = null;
		try {
			result = HdfsDefn.Result_File.parseFrom(new FileInputStream("file_protobuf"));
			for(HdfsDefn.File file : result.getFileList()) {
				System.out.println(file.getName());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error at list "+ e.toString());
			e.printStackTrace();
			//response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
			
		return result.toByteArray();
	}
	
	// Datanode <-> Namenode interaction methods
		
	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		System.out.println("NameNode sending blockReport");
		HdfsDefn.Result_Block.Builder resBlocks = HdfsDefn.Result_Block.newBuilder();
		try {
			HdfsDefn.File input = HdfsDefn.File.parseFrom(inp);
			HdfsDefn.Result_File proto = HdfsDefn.Result_File.parseFrom(new FileInputStream("file_protobuf"));
			for(HdfsDefn.File file : proto.getFileList()) {
				if(input.getName().equals(file.getName())) {
					ArrayList<HdfsDefn.Block> blockList = (ArrayList<HdfsDefn.Block>) file.getChunksList();
					resBlocks.addAllBlock(blockList);
				}
			}
		} catch (IOException e) {
			System.err.println("Error at blockReport "+ e.toString());
			e.printStackTrace();
			//response.setStatus(HdfsDefn.DataNode.Status.DEAD);
		}
		return resBlocks.build().toByteArray();
	}
	
	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		System.out.println("NameNode sending heartBeat");
		HdfsDefn.DataNode.Builder response = HdfsDefn.DataNode.newBuilder();
		
			try {
				HdfsDefn.DataNode dd = HdfsDefn.DataNode.parseFrom(inp);
				response.setSName(dd.getSName()); //ipaddress  (read config?)
				//response.setReplicas(dd.getReplicas());
				response.setStatus(HdfsDefn.DataNode.Status.ALIVE);
				response.setTimestamp(dd.getTimestamp());
				
				try {
					HdfsDefn.Result_DataNode fileDn = HdfsDefn.Result_DataNode.parseFrom(new FileInputStream("dn_protobuf"));
					for(HdfsDefn.DataNode dn : fileDn.getDatanodeList()) {
						if(dd.getSName().equals(dn.getSName())) {
							//datanode already in file
							return response.build().toByteArray();
						}
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				//add datanode to file
				HdfsDefn.DataNode result = response.build();
				synchronized(this) {
					try {
						FileOutputStream output = new FileOutputStream("dn_protobuf", true);
						result.writeTo(output);
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				//if datanode is dead
				//delete blocks in block list (parse file_protobuf to change status of datanode)
				//update file with delete
				
			} catch (InvalidProtocolBufferException e) {
				e.printStackTrace();
			}
		
		
		return response.build().toByteArray();
	}
	
	public void printMsg(String msg)
	{
		System.out.println(msg);		
	}
	
	public Runnable blockRunnable = new Runnable() {
		@Override
		public void run() {
			try {
				blockReport(null);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			
		}
	};
	
	public int getValuefromConfig(String name){
        int value = 0;
		try(Reader reader = Files.newBufferedReader(Paths.get("config.properties"), StandardCharsets.UTF_8)) {
			Properties properties = new Properties();
			properties.load(reader);
			value = Integer.valueOf(properties.getProperty(name));
			return value;   
        }catch(Exception e){
            System.out.println("Could not load value");
            
        }
        return value;
    }

    public static String[] readConfig(String filename){
        BufferedReader objReader = null;
        String [] config_split = null;
        try {
            String strCurrentLine;

            objReader = new BufferedReader(new FileReader(filename));

            while ((strCurrentLine = objReader.readLine()) != null) {
                ArrayList<String> configDetails = new ArrayList<String>();
                configDetails.add(strCurrentLine);
        	    if(configDetails.size()>0) {
        		String to_split = configDetails.get(0);
        		config_split = to_split.split(";");
                //System.out.println(strCurrentLine);	
        	    }
    	    }
            return config_split;

        } catch (IOException e) {

            e.printStackTrace();

        } finally {

	        try {
	            if (objReader != null)
	            objReader.close();
	        } catch (IOException ex) {
	            ex.printStackTrace();
	        }
        }
		return config_split;
    	
	}

    public static File getFilePath(String filename){
        String filepath = "";
        String appendFile = "";
        File f = null;
        if(new File(filename).isAbsolute()){
            f = new File(filename);
        } else{
            filepath = new File("").getAbsolutePath();
            appendFile = filepath + "/" + filename;
            f = new File(appendFile);
        }
        return f;
    }

	public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException
	{
		
		String [] params = readConfig(getFilePath("nn_config.txt").getPath());
		String name = params[0];
		String ip = params[1];
		int port = Integer.valueOf(params[2]);
		
		NameNode obj = new NameNode(ip, port, name);
		
	    INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);
	    serverRegistry = LocateRegistry.createRegistry(port);
	    try {
			serverRegistry.bind("INameNode", stub);
		} catch (AlreadyBoundException e) {
			e.printStackTrace();
		}
        System.err.println("Server ready");
        int blockReportInterval = obj.getValuefromConfig("blockReportInterval");    
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        //thread that receives heart beat
        //executor.scheduleAtFixedRate(obj.blockRunnable, 0, blockReportInterval, TimeUnit.SECONDS);
            
            /*Thread tobj = new Thread(obj);
            tobj.start();*/
            
        
	}
	
}
