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
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Client
{
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub
    public Client()
    {
        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
        String [] params = readConfig(getFilePath("nn_config.txt"));
        //String name = params[0];
        String IP = params[1];
        int port = Integer.valueOf(params[2]);
    	this.NNStub = GetNNStub("INameNode", IP, port); //get parameters from config
    }

    public int getValuefromConfig(String name){
        try(Reader reader = Files.newBufferedReader(Path.get("config.properties"), StandardCharsets.UTF_8)) {
        Properties properties = new Properties();
        properties.load(reader);
        int value = Integer.valueOf(properties.getProperty(name));
        return value;   
        }catch(Exception e){
            System.out.println("Could not load value");
            continue;
        }
    }

    public String[] readConfig(String filename){
        BufferedReader objReader = null;
        try {
            String strCurrentLine;

            objReader = new BufferedReader(new FileReader(filename));

            while ((strCurrentLine = objReader.readLine()) != null) {
            ArrayList<String> configDetails = new ArrayList<String>();
            configDetails.add(strCurrentLine);
    	    if(configDetails.size()>0) {
    		String to_split = configDetails[0];
    		String[] config_split = to_split.split(";");
            //System.out.println(strCurrentLine);
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
        
    }
    }

    public File getFilePath(String filename){
        String filepath = "";
        String appendFile = "";
        File f = null;
        if(new File(filename).isAbsolute()){
            f = new File(filename);
        } else{
            filePath = new File("").getAbsolutePath();
            appendFile = filePath + "/" + filename;
            f = new File(appendFile);
        }
        return f;
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
            String [] params = readConfig(getFilePath("dn_config.txt"));
            //from config file
            String name = params[0];
            String IP = params[1];
            int port = Integer.valueOf(params[2]);
            DataNode dn = new DataNode(name, port, IP);
        	//DataNode dn = new DataNode("cp", 2005, "128.6.13.177"); //get from config
        	IDataNode writeDn = GetDNStub("IDataNode", IP, port); //get from config
        	sendFile.setName(Filename);
        	
            byte[] open = this.NNStub.openFile(sendFile.build().toByteArray());
        	HdfsDefn.File parseOpen = HdfsDefn.File.parseFrom(open);
        	
        	//returns file message with blocks and assigned datanodes
        	byte[] byteAssign = this.NNStub.assignBlock(sendFile.build().toByteArray());
        	
        	HdfsDefn.File parseFile = HdfsDefn.File.parseFrom(byteAssign);
        	HdfsDefn.File.Builder buildFile = HdfsDefn.File.newBuilder(parseFile);
			
        	buildFile.setHandle(parseOpen.getHandle());
        	
        	//writes file content into blocks
        	writeDn.writeBlock(buildFile.build().toByteArray());
        	
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
            String [] params = readConfig(getFilePath("dn_config.txt"));
            //from config file
            String name = params[0];
            String IP = params[1];
            int port = Integer.valueOf(params[2]);
            DataNode dn = new DataNode(name, port, IP);
        	//DataNode dn = new DataNode("cp", 2005, "128.6.13.177"); //get from config
        	IDataNode writeDn = GetDNStub("IDataNode", IP, port); //get from config
        	//IDataNode writeDn = GetDNStub("IDataNode", "128.6.13.177", 2005); //get parameters from config
        	sendFile.setName(Filename);

            byte[] open = this.NNStub.openFile(sendFile.build().toByteArray());
        	HdfsDefn.File parseOpen = HdfsDefn.File.parseFrom(open);
        	
        	
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
        	writeDn.readBlock(byteLocations);
        	
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
                System.out.println("2. get filename ## To get a file in HDFS"); 
                System.out.println("3. list ## To get the list of files in HDFS");
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
