package ds.hdfs;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IDataNode extends Remote{

	/* Method to read data from any block given block-number */
	byte[] readBlock(byte[] inp) throws RemoteException;
	
	/* Method to write data to a specific block */
	byte[] writeBlock(byte[] inp) throws RemoteException;	
}
