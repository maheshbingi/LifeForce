import comm_pb2
import socket               
import time
import struct
import os,sys
import Image
import ntpath
import datetime

UUID = "test"

def sendMsg(msg_out, port, host):
    s = socket.socket()         
    s.connect((host, port))        
    msg_len = struct.pack('>L', len(msg_out))    
    s.sendall(msg_len + msg_out)
    len_buf = receiveMsg(s, 4)
    msg_in_len = struct.unpack('>L', len_buf)[0]
    msg_in = receiveMsg(s, msg_in_len)
    
    r = comm_pb2.Request()
    r.ParseFromString(msg_in)
    s.close
    return r

def receiveMsg(socket, n):
    buf = ''
    while n > 0:        
        data = socket.recv(n)                  
        if data == '':
            raise RuntimeError('data not received!')
        buf += data
        n -= len(data)
    return buf  

def getFileName(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def buildWritePhotoRequest(imagePath, ownerId):
    jobId = str(int(round(time.time() * 1000)))
    r = comm_pb2.Request()
    f = open(imagePath, "r")

    fileData = None
    try:
        fileData = f.read()
    finally:
        f.close()

    print(getFileName(imagePath))

	# Request header
    r.header.photoHeader.requestType = comm_pb2.PhotoHeader.write
    r.header.photoHeader.contentLength = os.path.getsize(imagePath)

	# Request payload
    r.body.photoPayload.name = getFileName(imagePath)
    r.body.photoPayload.data = fileData

    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.originator = 0
    r.header.toNode = 0
    msg = r.SerializeToString()
    print("image write job built")
    return msg

def buildUpdatePhotoRequest(imagePath, ownerId):
    jobId = str(int(round(time.time() * 1000)))
    r = comm_pb2.Request()
    f = open(imagePath, "r")


    fileData = None
    try:
        fileData = f.read()
    finally:
        f.close()

    print(getFileName(imagePath))

	# Request header
    r.header.photoHeader.requestType = comm_pb2.PhotoHeader.write
    r.header.photoHeader.contentLength = os.path.getsize(imagePath)

	# Request payload
    r.body.photoPayload.name = getFileName(imagePath)
    r.body.photoPayload.uuid = UUID
    r.body.photoPayload.data = fileData

    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.originator = 0
    r.header.toNode = 0
    msg = r.SerializeToString()
    print("image write job built")
    return msg

def buildReadPhotoRequest(imageName, ownerId):
    jobId = str(int(round(time.time() * 1000)))
    r = comm_pb2.Request()

	# Request header
    r.header.photoHeader.requestType = comm_pb2.PhotoHeader.read

	# Request payload
    r.body.photoPayload.name = imageName
    r.header.routing_id = comm_pb2.Header.JOBS
    r.body.photoPayload.uuid = UUID
    r.header.originator = 0
    r.header.toNode = 0
    msg = r.SerializeToString()
    print("image read job built")
    return msg

def buildDeletePhotoRequest(imageName, ownerId):
    jobId = str(int(round(time.time() * 1000)))
    r = comm_pb2.Request()

	# Request header
    r.header.photoHeader.requestType = comm_pb2.PhotoHeader.delete

	# Request payload
    r.body.photoPayload.name = imageName
    r.body.photoPayload.uuid = UUID

    r.header.routing_id = comm_pb2.Header.JOBS
    r.header.originator = 0
    r.header.toNode = 0
    msg = r.SerializeToString()
    print("image read job built")
    return msg
   
if __name__ == '__main__':
    host = "localhost" #raw_input("IP:")
    port = 5573
    whoAmI = 1

    option = raw_input("Select option\n1 Read image\n2 Write image\n3 Update image\n4 Delete image\n\n")
    createImageJob = None

    if option == '1':
        createImageJob = buildReadPhotoRequest("input_image.jpg", 1)
    elif option == '2':
        createImageJob = buildWritePhotoRequest("input_image.jpg", 1)
    elif option == '3':
        createImageJob = buildUpdatePhotoRequest("input_image.jpg", 1)
    elif option == '4':
        createImageJob = buildDeletePhotoRequest("input_image.jpg", 1)
    else:
        print("In correct option")

    result = sendMsg(createImageJob, port, host)
    print(result)
