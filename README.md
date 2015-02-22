## LifeForce - Image storage system

### Photographs capture our travel through life. LifeForce's mission is to preserve, share, remember, connect, and reflect solely through pictures.


- Life force is a peer based distributed system which uses Netty for asynchronous communication between different nodes.
- Implemented database sharding.
- Cluster in ring topology supporting fault tolerance.
- Supports leader election between nodes for avoiding single point of failure.
- Used protobuf messages for management between nodes and also to transfer data from client to server.


### Technologies Used:
- Languages: Java, Python
- CorePackages: Google Protobuf, JBossNetty for communication
- Storage: PostgreSQL
