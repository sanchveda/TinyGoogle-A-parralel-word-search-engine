# first of all import the socket library 
import socket                
import numpy as np 
import pickle
import sys
import string
import sys

from support import *
from Invertedindex import *
from Invertedindex_reducer import *
from SearchQuery import *

server_list=get_server_list()

def announce_yourself():
   
   #----------Connection section------------------------#
   try :
      soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
   except socket.error as err:
      print ("Socket creation failed with error %s" %(err))

   try:
      host=socket.gethostbyname('neptunium.cs.pitt.edu')
      hostname,host_ip,port=get_address_from_ip(host,server_list)
     
   except socket.gaierror:
      #This could not resolve the host
      print ("There was an error resolving the host")
      sys.exit()

   print (host_ip,port)
   #raw_input("")
   try:
      soc.connect((hostname, int(port)))
   except:
      print("Connection error")
      sys.exit() 
   #----------------------Connection established----------------------#
   
   #---------------------Server will now send the NEW packet to the Masterserver---#
   #tup=("NEW",socket.gethostname())
   #send_packet=pickle.dumps(tup)
   #soc.send(send_packet)
   data="NEW"+";;;"+socket.gethostname()
   send_one_msg(soc,data)
   #---------------------Server will now receive a packet fromt the Master stating whether it is added or denied------#
   #recv_packet=soc.recv(1024)
   #recv_msg=pickle.loads(recv_packet) #recv_msg will be of type ("CON",Messaget_Text)

   recv_packet=recv_one_message(soc)
   recv_msg=recv_packet.split(";;;")

   if recv_msg[0] == "CON_GRANTED":
      print("Server added to the worker nodes")
      soc.close()
      return True
   elif recv_msg[0] == "CON_EXISTS":
      print("Server already exist in the worker list")
      soc.close()
      return False
   else:
      print ("There has been some failure")
      sys.exit(1)

   print (recv_msg)
   #raw_input("")
   
   #soc.close()
###----------------------------End of function-----------------------------###


##--------- The below function is the function which handles what to do once it receives the request from the client ----
##----It is not called as a thread function but is a standalone function created to simply the code of the main function--###
def action(clientsocket,addr):

   while True:
      #rec_msg=clientsocket.recv(1024)
      rec_msg=recv_one_message(clientsocket)
      msg=rec_msg.split(';;;')
      #msg=pickle.loads(rec_msg)
      #raw_input("")

      if msg[0]=="MAP":
         #print (msg[1])
         
         second_part=pickle.loads(msg[1])
         
         filepath,start,end,outputpathname=second_part.split(';')
         print (filepath,start,end,outputpathname)
         #raw_input("")
         index_dict=invertedindex_mapper(filepath,int(start),int(end))

         #disp_dict(index_dict)
         write_file(outputpathname,index_dict)
         #raw_input("")
         #x=get_data()
         #data=io.BytesIO(x)
         #chunk=data.read(1024)
         
         #tup=("DONE_MAP","Mapping is done in worker")
         data="DONE_MAP"+";;;"+"Mapping is done in worker"
         #send_packet=pickle.dumps(tup)
         send_one_msg(clientsocket,data)
         #clientsocket.send(send_packet)
         break
      
      elif msg[0] =="REDUCE":
         second_part=pickle.loads(msg[1])
         inputdir,start,end,outputpathname=second_part.split(';')
         print (inputdir,start,end)

         index_dict=invertedindex_reducer(inputdir,start,end)

         write_file(outputpathname,index_dict)

         disp_dict(index_dict)
         
         data="DONE_REDUCE"+";;;"+"Mapping is done in worker"
         #send_packet=pickle.dumps(tup)
         send_one_msg(clientsocket,data)
         #clientsocket.send(send_packet)
         
         break

      elif msg[0] == "SEARCH":

         second_part=msg[1]
         #print (second_part)
         keyword,filepathname=second_part.split(';')

         r_host,r_ip,r_port=get_address_from_ip(addr[0],server_list)
         #print ("Received from ",r_ip,r_port,"Message:",second_part)

         #print (keyword,filepathname)
        

         search_dict=searchword(filepathname,keyword)
         if search_dict != "":
            #print (search_dict)
            content=pickle.dumps(search_dict)
            data="DATA_SEND"+";;;"+content
            #tup=("DATA",search_dict)
            #send_packet=pickle.dumps(data)
            
         else:
            #print ("Nothing is found")
            content=pickle.dumps("Not found")
            data="DATA_NOT_FOUND"+";;;"+content
            #tup=("DATA","Nothing")
            #send_packet=pickle.dumps(data)
            
         send_one_msg(clientsocket,data)
         #raw_input("")

      
         #some_result=some_search(keyword,filepathname)
         #tup=("DONE_SEARCH","Done searching in worker")
         data="DONE_SEARCH"+";;;"+"Done searching in worker"
         #send_packet=pickle.dumps(tup)
         #clientsocket.send(send_packet)
         send_one_msg(clientsocket,data)
         break
      else:
         print("You are now in the else part")
         break

   ### Here you can send some message to the masterserver
   clientsocket.close()


   
def main():

   inp=raw_input("Do you want to add this to the worker server(yes/no)->")
   
   if string.upper(inp) == "YES" or string.upper(inp) == "Y": 
      permission=announce_yourself() 
      #print("Return to main")
      #raw_input("")

      if permission:
         print ("%s has the permission. It will now try to be connect with master server" %(socket.gethostname()))
            # next create a socket object 
      else:
         print ("%s is already part of the initial worker servers." %(socket.gethostname()))

      s = socket.socket()          
      print "Socket successfully created"
        
      # reserve a port on your computer in our 
      # case it is 12345 but it can be anything 
      hostname,host_ip,port=get_address(socket.gethostname(),server_list)

      s.bind((host_ip,int(port)))

        
      # Next bind to the port 
      # we have not typed any ip in the ip field 
      # instead we have inputted an empty string 
      # this makes the server listen to requests  
      # coming from other computers on the network 
      print ("Binding happened with",host_ip,port) 
        
      # put the socket into listening mode 
      s.listen(5)      
      print "socket is listening"            
        
      # a forever loop until we interrupt it or  
      # an error occurs 
      while True: 
         # print ("Things are here")

         # Establish connection with client. 
         c, addr = s.accept()      
         
         #print ("However address is ",addr)
         action(c,addr)
                  
      s.close()
   else:
      print ("Exiting")
      sys.exit(0)         


if __name__ == "__main__":
   main()
