package ds.hdfs;

import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class NetworkTest implements temporaryInterface{

	protected Registry serverRegistry;
	String name;
	public NetworkTest(String s) throws RemoteException {
		super();
		this.name = s;
		
	}
	
	public static void main(String[] args) throws RemoteException,InterruptedException, UnknownHostException{
		// TODO Auto-generated method stub

		System.setProperty("java.rmi.server.hostname", "10.1.39.133");
		System.setProperty("java.security.policy","test.policy");

        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        		
		final NetworkTest serverObj = new NetworkTest(args[0]);
		try {
			temporaryInterface stub = (temporaryInterface) UnicastRemoteObject.exportObject(serverObj, 0);

			// Bind the remote object's stub in the registry
			serverObj.serverRegistry = LocateRegistry.getRegistry(2002);
			serverObj.serverRegistry.bind(serverObj.name, stub);

			System.err.println(serverObj.name + " ready");
		} catch (Exception e) {
			System.err.println("Server exception: " + e.toString() + " Failed to start server");
			e.printStackTrace();
		}
		
		boolean found = false;
		
		while(!found)
		try {
			Registry registry2 = LocateRegistry.getRegistry("10.1.39.21",2002);
			temporaryInterface stub2 = (temporaryInterface) registry2.lookup(args[1]);
			
			System.out.println("Connected to other guy");
			found = true;
			
		} catch (Exception e) {
						
			TimeUnit.SECONDS.sleep(1);
			System.out.println("Still searching");
			e.printStackTrace();
		}
		
	}

}
