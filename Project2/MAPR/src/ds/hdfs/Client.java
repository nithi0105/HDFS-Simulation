package ds.hdfs;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
//import ds.hdfs.hdfsformat.*;
import com.google.protobuf.ByteString; 
import ds.hdfs.INameNode;
import ds.hdfs.HdfsDefn.Block;
import ds.hdfs.IDataNode;

public class Client
{
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    public Client()
    {
        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
    	this.NNStub = GetNNStub("INameNode", "128.6.13.175", 2007); //get parameters from config
    }

    public IDataNode GetDNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                System.out.println("DataNode Found!");
                return stub;
            }catch(Exception e){
            	System.out.println("DataNode still not found");
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
                System.out.println("NameNode Found!");
                return stub;
            }catch(Exception e){
            	System.out.println("NameNode still not found");
                continue;
            }
        }
    }

    public void PutFile(String Filename) //Put File
    {
        System.out.println("Going to put file " + Filename);
        BufferedInputStream bis;
        HdfsDefn.File.Builder sendFile = HdfsDefn.File.newBuilder();
        try{
        	DataNode dn = new DataNode("cp", 2005, "128.6.13.177"); //get from config
        	IDataNode writeDn = GetDNStub("IDataNode", "128.6.13.177", 2005); //get from config
        	sendFile.setName(Filename);
        	
        	byte[] open = this.NNStub.openFile(sendFile.build().toByteArray());
        	HdfsDefn.File parseOpen = HdfsDefn.File.parseFrom(open);
        	bis = new BufferedInputStream(new FileInputStream(String.valueOf(parseOpen.getHandle())));
        	
        	//returns file message with blocks and assigned datanodes
        	byte[] byteAssign = this.NNStub.assignBlock(sendFile.build().toByteArray());
        	
        	HdfsDefn.File parseFile = HdfsDefn.File.parseFrom(byteAssign);
        	HdfsDefn.File.Builder buildFile = HdfsDefn.File.newBuilder(parseFile);
			
        	
        	int numByte = bis.available();
        	int blockBytes = 64; //configurable
        	int start = 0;
        	
        	int index = 0;
			for(HdfsDefn.Block block : buildFile.getChunksList()) {
				HdfsDefn.Block.Builder chunk = HdfsDefn.Block.newBuilder();
				
	        	byte[] content = new byte[numByte];
	        	bis.read(content, start, blockBytes);
	        	start = blockBytes+1;
	        	String str = new String(content, "UTF-8");
	        	
				chunk.setName(block.getName());
				chunk.addAllDatanodes(block.getDatanodesList());
				chunk.setContent(str);
				
				//given a chunk with data, return the chunk with datanodes with data
				byte[] response = writeDn.writeBlock(chunk.build().toByteArray());
				HdfsDefn.Block parseChunk = HdfsDefn.Block.parseFrom(response);
				buildFile.setChunks(index, parseChunk);
				index++;
			}
        	
			//add file to proto file
        	HdfsDefn.File result = buildFile.build();
			try {
				FileOutputStream output = new FileOutputStream("file_protobuf", true);
				result.writeTo(output);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
        	bis.close();
        }catch(Exception e){
            System.out.println("File not found !!!");
            return;
        }
    }

    public void GetFile(String Filename)
    {
        System.out.println("Going to get file " + Filename);
        BufferedInputStream bis;
        HdfsDefn.File.Builder sendFile = HdfsDefn.File.newBuilder();
        try{
        	DataNode dn = new DataNode("cp", 2005, "128.6.13.177"); //get parametes from config
        	IDataNode writeDn = GetDNStub("IDataNode", "128.6.13.177", 2005); //get parameters from config
        	sendFile.setName(Filename);
        	
        	byte[] open = this.NNStub.openFile(sendFile.build().toByteArray());
        	HdfsDefn.File parseOpen = HdfsDefn.File.parseFrom(open);
        	bis = new BufferedInputStream(new FileInputStream(String.valueOf(parseOpen.getHandle())));
        	
			HdfsDefn.Result_File resFile = HdfsDefn.Result_File.parseFrom(new FileInputStream("file_protobuf"));
			HdfsDefn.Result_Block.Builder response = HdfsDefn.Result_Block.newBuilder();
			ArrayList<HdfsDefn.Block> blockList = null;
			for(HdfsDefn.File file : resFile.getFileList()) {
				if(file.getName().equals(Filename)) {
					blockList = (ArrayList<HdfsDefn.Block>) file.getChunksList();
				}
			}
			response.addAllBlock(blockList);
			
			//returns datanode list
        	byte[] byteLocations = this.NNStub.getBlockLocations(response.build().toByteArray());
        	HdfsDefn.Result_DataNode parseResponse = HdfsDefn.Result_DataNode.parseFrom(byteLocations);
        	for(HdfsDefn.DataNode datanode : parseResponse.getDatanodeList()) {
        		writeDn.readBlock(datanode.toByteArray());
        		//write to local file
        	}
        	bis.close();
        }catch(Exception e){
            System.out.println("File not found !!!");
            return;
        }
    }

    public void List()
    {
    }

    public static void main(String[] args) throws RemoteException, UnknownHostException
    {
        // To read config file and Connect to NameNode
        //Intitalize the Client
        Client Me = new Client();
        System.out.println("Welcome to HDFS!!");
        Scanner Scan = new Scanner(System.in);
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.nextLine();
            String[] Split_Commands = Command.split(" ");

            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS"); System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))  // put Filename
            {
                //Put file into HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.PutFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Get file from HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.GetFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Get list of files in HDFS
                Me.List();
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}
