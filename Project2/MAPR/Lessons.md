what you learnt, what you implemented, pseudo code

This project really opened our eyes to the architecture of the Hadoop Distributed File System. We really got to see all the internal components that go into storing files on a system and sending it back to the user (where the user typically does not see this internal processes).

We used Java rmi for server/client connection 

Implemented: (Gianna Cortes worked on NameNode and Client, Nithi Kumar worked on DataNode and Client)
-The use of configurable files to change parameters.
-Server/client connection with Java rmi
-Client connection with DataNode and NameNode
 	- for Putfile: 
		- calling NameNode to assign blocks 
		- calling DataNode to write into those blocks
	- for Getfile: 
		- calling NameNode to find the blocks
		- calling DataNode to read the blocks 
-DataNode heart beat
-NameNode block report

