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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

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
	//method that takes in blocklocation of chunk to read and returns contents of block in byte array
   	 public byte[] readBlock(byte[] Inp)
   	 {
        HdfsDefn.File.Builder retFile = HdfsDefn.File.newBuilder();
        try
        {
        	HdfsDefn.File f = HdfsDefn.File.parseFrom(Inp);
        	HdfsDefn.Result_DataNode parseResponse = HdfsDefn.Result_DataNode.parseFrom(Inp);
        	for(HdfsDefn.DataNode datanode : parseResponse.getDatanodeList()) {
        		//write to return file 
			retFile.setContent("This is the content of the datanode");
			
        	}
        }
        catch(Exception e)
        {
            System.out.println("Error at readBlock");
            //response.setStatus(-1);
        }

        return retFile.build().toByteArray();
    }
	
	//given a datanode, writes content from file into datanode and returns index of the chunk
    	public byte[] writeBlock(byte[] Inp)
    	{
        BufferedInputStream bis;
    	HdfsDefn.File.Builder retFile = HdfsDefn.File.newBuilder();
        try
        {
        	HdfsDefn.File f = HdfsDefn.File.parseFrom(Inp);
        	bis = new BufferedInputStream(new FileInputStream(String.valueOf(f.getHandle())));
        	
        	int numByte = bis.available();
        	int blockBytes = getValuefromConfig("blockBytes"); //configurable
        	int start = 0;
        	
        	int index = 0;
        	//write content into each block in the file's chunk list
			for(HdfsDefn.Block block : f.getChunksList()) {
				HdfsDefn.Block.Builder chunk = HdfsDefn.Block.newBuilder();
				
	        	byte[] content = new byte[numByte];
	        	bis.read(content, start, blockBytes);
	        	start = blockBytes+1;
	        	String str = new String(content, "UTF-8");
	        	
				chunk.setName(block.getName());
				chunk.addAllDatanodes(block.getDatanodesList());
				chunk.setContent(str);
				
				retFile.setChunks(index, chunk.build());
				index++;
			}
			
			//add file to proto file
        	HdfsDefn.File result = retFile.build();
			try {
				FileOutputStream output = new FileOutputStream("file_protobuf", true);
				result.writeTo(output);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
        	bis.close();
        	
        	System.out.println("in writeBlock");
        }
        catch(Exception e)
        {
            System.out.println("Error at writeBlock ");
            //response.setStatus(-1);
        }

        return retFile.build().toByteArray();
    }

	//configures heart Beat message as Name, IP, and port as byte array
    	public byte[] heartBeat(String name, String IP, int port) throws RemoteException{
        HdfsDefn.DataNode.Builder response = HdfsDefn.DataNode.newBuilder();
        response.setSName(name);
        response.setAddress(IP);
        response.setPort(port);
        response.setStatus(HdfsDefn.DataNode.Status.ALIVE);
        return response.build().toByteArray();
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
	//method to access configurable variables
	//given name of variable, returns int of the value
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
	//method to access parameters from dn_config.txt or nn_config.txt
    	public String[] readConfig(File filename){
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
	
	// method to find file path if outside current directory
    	public File getFilePath(String filename){
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


    public INameNode GetNNStub(String Name, File filename)
    {
        while(true)
        {
            try
            {
                String [] config_split = this.readConfig(filename);
                //String Name = config_split[0];
                String IP = config_split[1];
                int Port = Integer.valueOf(config_split[2]);
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

    /*public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                String [] config_split = readConfig("nn_config.txt");

                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("NameNode Found!");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }*/

    public static void main(String args[]) throws InvalidProtocolBufferException, IOException
    {
        //Define a Datanode Me
    	DataNode Me = null;
        String [] params = Me.readConfig(Me.getFilePath("dn_config.txt"));
        String name = params[0];
        String IP = params[1];
        int port = Integer.valueOf(params[2]);
        Me = new DataNode(name, port, IP);
        //DataNode Me = new DataNode("cp", 2005, "128.6.13.177"); //get from dn config?
        //INameNode stub = Me.GetNNStub("INameNode", "128.6.13.175", 2007);
        INameNode stub = Me.GetNNStub("INameNode", Me.getFilePath("nn_config.txt")); //get from nn config?
        Me.NNStub = stub;
        int heartBeatTime = Me.getValuefromConfig("heartBeatTime");
        Me.BindServer("IDataNode", IP, port); //get from config
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        
	//thread that sends heart beat specified by heartBeatTime
        final DataNode dn = Me;
        executor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                    try {
						dn.heartBeat(dn.MyName, dn.MyIP, dn.MyPort);
					} catch (RemoteException e) {
						e.printStackTrace();
					}
                }
	    }, 0, heartBeatTime, TimeUnit.SECONDS);
    }
}
