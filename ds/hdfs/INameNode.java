package ds.hdfs;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INameNode extends Remote{

	/* Method to open a file given file name with read-write flag*/
	byte[] openFile(byte[] inp) throws RemoteException;
	
	byte[] closeFile(byte[] inp ) throws RemoteException;
	
	/* Method to get block locations given an array of block numbers */
	byte[] getBlockLocations(byte[] inp ) throws RemoteException;
	
	/* Method to assign a block which will return the replicated block locations */
	byte[] assignBlock(byte[] inp ) throws RemoteException;
	
	/* List the file names (no directories needed for current implementation */
	byte[] list(byte[] inp ) throws RemoteException;
	
	/*
		Datanode <-> Namenode interaction methods
	*/
	
	/* Get the status for blocks */
	byte[] blockReport(byte[] inp ) throws RemoteException;
	
	/* Heartbeat messages between NameNode and DataNode */
	byte[] heartBeat(byte[] inp ) throws RemoteException;
}
