#---------------------------TINY GOOGLE------------------------------#
#-AUTHOR: SANCHAYAN SARKAR-------------------------Date-12/8/2018-----#
#-Version1.0--------

import socket                
import thread
import os
import sys
import pickle
import numpy as np
import string
from threading import Thread
import io
import shutil
import time

global server_list
global worker_list

from support import *
from Split_file import *
from reducer_split_file import *
from SearchQuery import *

server_list=get_server_list()
worker_list={}
#index_queue=[]

#------------------Create the list of servers--------------------- 
def create_server_list(filename):

	domain='.cs.pitt.edu'
	
	#server_list={}

	
	items=[]

	with open(filename) as fp:
		lines=fp.readlines()
		
		for line in lines:
			line_split=line.rstrip().split(';')
			items.append(line_split)

	for i in range (len (items[0])):
		server_name=items[0][i]+domain
		server_ip_address=socket.gethostbyname(server_name)
		server_portnumber=items[1][i]
		server_list.update({server_name:(server_ip_address,server_portnumber)})

	with open("serverlist.p",'w') as fp :
		pickle.dump(server_list,fp)
		print ("Serverlist successfully created")

	#server_list['hostname']=[items[0][i]+domain for i in range(len(items[0]))]
	#server_list['portnumber']=[items[1][i] for i in range(len(items[1]))]
	
#----------------------------------------------------------------------------



#-------This function exclusively updates the worker_list#
def add_server_to_worker(clientsocket,hostname):

	flag=update_worker_list(hostname)

	disp_dict(worker_list)

	if flag == 1:
		print ("NEW_SERVER_REQUEST = GRANTED")
		#tup=("CON_GRANTED","Host added to worker host")
		data="CON_GRANTED"+";;;"+"Host added to worker host list. You can now make your connection"
	else:
		print ("NEW_SERVER_REQUEST = DENIED.Sending message to the client")
		#tup=("CON_DENIED","Host already exists in the worker list")
		data="CON_EXISTS"+";;;"+"Host already is there in worker list. You already have a conenction"

	#send_packet=pickle.dumps(tup)
	#clientsocket.send(send_packet)
	send_one_msg(clientsocket,data)

def mapper_code(clientsocket,filepathlist,number_of_workers,outputdir,counter):
	number_of_workers=len(worker_list)
	print (number_of_workers)
	queue=[]

	#---------This mapper code--------------#
	for filepath in filepathlist:
		## size of file 
		## And then divide the file by the number of worker_node
		threads=[]
		#index=0
		#queue=[]
		#print ("filepath=",filepath)
		split_list=split_file(filepath,number_of_workers)
		idx_count=0
		outputfilename=""
		for keys,values in worker_list.items():
			hostname=keys
			host_ip=values[0]
			portnumber=values[1]
			
			start,end=split_list[idx_count] 
			
			outputfilename=keys+"_"+extract_filename(filepath)+"_split"+str(idx_count)+"_"+counter+"_"+".p" #will give server_filepath_split_0
			outputpathname=os.path.join(outputdir,outputfilename)
			print (outputpathname)
			#raw_input("")

			t=Thread(target=index_thread,args=[hostname,host_ip,portnumber,"MAP",filepath,"",start,end,outputpathname,queue])
			t.start()
			threads.append(t)
		
			idx_count=idx_count+1 
			#thread.start_new_thread(server_thread,(hostname,host_ip,portnumber,)) #The splits will also go along with this
		for process in threads:
			process.join()

		print ("Threads done for===",filepath,"Queue",queue,idx_count)

	return queue

def reducer_code(clientsocket,number_of_workers,tempdir,indexdir):

	queue=[]
	threads=[]
	#------------------This reducer code --------------------
	split_list=split_InvertedIndex_reducer(number_of_workers)
	idx_count=0
	
	for keys,values in worker_list.items():
		hostname=keys
		host_ip=values[0]
		portnumber=values[1]

		start,end=split_list[idx_count]

		outputfilename=keys+"_"+"final_"+str(idx_count)+".p"  #Any filename is possible
		outputpathname=os.path.join(indexdir,outputfilename)

		t=Thread(target=index_thread,args=[hostname,host_ip,portnumber,"REDUCE","",tempdir,start,end,outputpathname,queue])
		t.start()
		threads.append(t)

		idx_count=idx_count+1

	for process in threads:
		process.join()

	print ("Threads done for==",tempdir,"Queue",queue,idx_count)

	return queue
	

#----This function will call server thread to connect to the worker nodes-------#
def index_driver(clientsocket,filepathlist,counter):


	outputdir="temp"
	indexdir="index"
	
	###	'he
	# next section of code is to create the temporary and the index directories
	if os.path.isdir(outputdir):
		'''
		try:
			shutil.rmtree(outputdir)
		except OSError as err:
			print ("Cannot remvoe the existing temp directory folder because of %s" %(err.filename))
		'''
		a=0
	else:
		try:
			os.mkdir(outputdir)
		except:
			print ("Cannot create a new temporary directory")
		#-======================================

	if os.path.isdir(indexdir):
		try:
			shutil.rmtree(indexdir)
		except OSError as err:
			print ("Cannot remove the existing folder because of %s" %(err.filename))
	
	try:
		os.mkdir(indexdir)
	except:
		print("Cannot create the index directory")
	### Code for creating and indexing directories ends here 
	

	number_of_workers=len(worker_list)
	
	queue_map=mapper_code(clientsocket,filepathlist,number_of_workers,outputdir,counter) # This will take care of the entire mapping section 
	print ("Mapping is done")
	print ("Code is here")
	
	queue_reduce=reducer_code(clientsocket,number_of_workers,outputdir,indexdir)
	
	print ("Reducing is done....")

	#global index_queue
	index_queue=queue_reduce

	with open ("index_queue.p","wb") as fp:
		pickle.dump(index_queue,fp)
	#remove the temporary directory
	print(index_queue)

	print ("Indexing done")
	#raw_input("")
	
	#tup=("INDEX_COMPLETE","Indexing is done")
	data="INDEX_COMPLETE"+";;;"+"Indexing is done"
	#send_packet=pickle.dumps(tup)
	send_one_msg(clientsocket,data)


#------This is searcherr--------------------------------#
def search_driver(clientsocket,searchwords):
	#print ("Inside search driver")
	splitname=[]
	busy_list=[]
	
	
	with open("index_queue.p","rb") as fp:
		index_queue=pickle.load(fp)
	start_time=time.time()
	#print (index_queue)
	#raw_input("")
	#print (index_queue)

	#print (len(index_queue))
	for word in searchwords:
		splitname.append(get_split(word,index_queue))
	
	#print("Searchwords",searchwords)
	#print ("Splitname",splitname)
	#print ("Workerlist",worker_list)
	#raw_input("")
	idx_count=0 #This is an index counter for that particular server in the worker list
	threads=[]
	queue=[]

	#print (len(searchwords))
	searchword_index=0
	#for searchword_index in range(len(searchwords)):
	while searchword_index < len(searchwords):
		#print ("Searchword_Index",searchword_index)
	
		flag =True

		
		if idx_count > (len(worker_list)-1):
			idx_count=0
			flag=False


		hostname=worker_list.keys()[idx_count]
		host_ip,portnumber=worker_list.values()[idx_count]

		

		while idx_count<(len(worker_list)-1):
			if does_exist(hostname,busy_list,value=True):
				idx_count=idx_count+1
			else:
				break
			try :
				hostname=worker_list.keys()[idx_count]
				host_ip,portnumber=worker_list.values()[idx_count]
			except:
				print ("For this index",idx_count)

		
		
		if idx_count > (len(worker_list)-1):
			idx_count=0
			flag=False
		

		if flag == True:
			#print ("This is where it will be sent too ",hostname,idx_count,searchwords[searchword_index])
			t=Thread(target=search_thread,args=[hostname,host_ip,portnumber,searchwords[searchword_index],splitname[searchword_index],idx_count,queue])
			t.setName(hostname+";;;"+str(searchword_index))
			#print ("Index",searchword_index)
			t.start()
			threads.append(t)
			busy_list.append(t.getName())
			searchword_index=searchword_index+1
			idx_count=idx_count+1


		for process in threads:
			if not process.isAlive():
				process.handled=True
			else:
				process.handled=False

		#threads=[process for process in threads if not process.handled]
		busy_list=[process.getName() for process in threads if not process.handled]
		#print (busy_list)
	
	end_time=time.time()
	#print (len(threads))
	for process in threads:
		process.join()

	#print ("Threads done for all words","Queue",queue)
	#print ("Results are obtained now")

	for queue_items in queue:
		p=pickle.dumps(queue_items)
		data="RESULT"+";;;"+p
		send_one_msg(clientsocket,data)
	
	#print ("Searching done")

	data="SEARCH_COMPLETE"+";;;"+str((end_time-start_time))
	send_one_msg(clientsocket,data)
#---------------This funtion is a thread for only for a SEARCH REQUEST from the server-------------------------#
def search_thread(hostname,host_ip,portnumber,keyword,filename,index_count,queue):
	try:
		soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	except socket.error as err:
		print ("Socket creation failed inside thread")

	try:
		soc.connect((host_ip,int(portnumber)))
	except:
		print ("Connection error inside thread")
		sys.exit()

	message=keyword+";"+filename
	#tup=("SEARCH",message)  ## This will be replaced by the actual message which would include the keyword to be searched
	data="SEARCH"+";;;"+message
	#send_packet=pickle.dumps(tup)


	#soc.send(send_packet)
	send_one_msg(soc,data)

	recv_packet=recv_one_message(soc)
	header,content=recv_packet.split(';;;')
	#recv_packet=soc.recv(1024)
	#ack=pickle.loads(recv_packet)
	#print ("Acknowledgement",ack)
	
	actual_data=""
	while header != "DONE_SEARCH": #When searching is done it will send another packet called "Done"
		
		
		#unpickle=pickle.loads(content)
		if header== "DATA_NOT_FOUND":
			actual_data=pickle.dumps(None)
			break

		actual_data=actual_data+content #This is going to accumulate the full content
		#recv_packet=soc.recv(1024)
		recv_packet=recv_one_message(soc)
		header,content=recv_packet.split(';;;')
		
		#ack=pickle.loads(recv_packet)
	
	real_data=pickle.loads(actual_data) #Unpickles the content. This will either be the dictionary or nothing
	
	#print ("Now_recvpacket",recv_packet)
	#soc.send("Thank You")
	'''	
	if header =="DONE_SEARCH":
		print ("Closing socket")
	'''

	#print ("PRINTING",real_data)
	
	queue.append((keyword,real_data)) # This is just a dummy statement for checking whether the values are recorded in the datastructure sent

	soc.close()



#----------Client Thread Function-------------#
def client_thread(clientsocket,address):
	
	while True:
		#rec_msg=clientsocket.recv(1024)
		rec_msg=recv_one_message(clientsocket)
		msg=rec_msg.split(";;;")
		#msg=pickle.loads(rec_msg)

		

		#NEW in the request means it is coming from a newly entered server. Immediately update the worker_list
		if msg[0] == "NEW":
			print ("Inside main thread",msg[1])
			hostname=msg[1]
			add_server_to_worker(clientsocket,hostname)
			break
			#print (msg[0],msg[1])

		elif msg[0]=="INDEX":
			#filepathlist,counter=msg[1].split(';')
			filepathlist=pickle.loads(msg[1])
			counter=msg[2]
			print (filepathlist)
			print (counter)
			#raw_input("")
			index_driver(clientsocket,filepathlist,counter)
			

		elif msg[0]=="SEARCH":

			#print (worker_list)

			searchwords=pickle.loads(msg[1])
			#print ("Inside mainserver listerner thread",searchwords)
			search_driver(clientsocket,searchwords)
			

		elif msg[0]=="EXIT":
			print (msg[1])
			#raw_input("")
			#print ("Closing the connection of client as from address %s is closing" %(address[0]))
			break
		
	try:
		clientsocket.close()
		print ("Client connection from address %s is closed" %(address[0]))
	except:
		print ("Unable to close port")
		sys.exit(1)
#------------------------------------------------#

def setup_initial_worker(filename):

	items=[]
	with open(filename,'r') as fp:
		lines=fp.readlines()

		for line in lines:
			line_split=line.rstrip().split(";")
			items.append(line_split)

	for i in range (len(items[0])):
		hostname,host_ip,portnumber=get_address(items[0][i],server_list)
		worker_list.update({hostname:(host_ip,portnumber)})

#---------------------------------------------------------------#

def update_worker_list(hostname):

	for key,values in worker_list.items():

			if key == hostname : 
				#If the host does not  exist 
				print ("Already exists")
				return 0

	host,ip,portnumber=get_address(hostname,server_list)
	worker_list.update({host:(ip,portnumber)})
	
	return 1
#-------------------------------------------------------------#

def index_thread(hostname,host_ip,port,message_header,filepath,outputdir,start,end,outputpathname,queue):

	try:
		soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	except socket.error as err:
		print ("Socket creation failed inside thread")

	#print ("Inside Server Thread")
	#print (hostname,host_ip,port)
	
	try:
		soc.connect((host_ip,int(port)))
		#print (host_ip,port)
		#raw_input("")
	except:
		print ("Connection error inside thread")
		sys.exit()

	if message_header=="MAP":
		message_content=filepath+";"+str(start)+";"+str(end)+";"+outputpathname
	elif message_header=="REDUCE":
		message_content=outputdir+";"+start+";"+end+";"+outputpathname

	
	#tup=(message_header,message_content)  ## This will be replaced by the ("INDEX","pathname;start;end")
	p=pickle.dumps(message_content)
	data=message_header+";;;"+p
	send_one_msg(soc,data)

	
	recv_packet=recv_one_message(soc)
	header,content=recv_packet.split(";;;")

	#recv_packet=soc.recv(1024)
	#ack=pickle.loads(recv_packet)

	#print("Code is here ")
	#print ("Acknowledgement",ack)
	#raw_input("")
	res_data=""
	while header != "DONE_MAP" and header != "DONE_REDUCE":
													 #When searching is done it will send another packet called "Done"
		
		# The following print statement will be replaced by the actual action where it stores the returned list of the search
		print ("Message",ack[0],ack[1])

		'''
		size=1024
		if string.upper(ack[0]) == "NEXT":
			size=int(ack[1])
		
		recv_packet=soc.recv(size)
		ack=pickle.loads(recv_packet)
		if string.upper(ack[0]) =="DATA":	
			res_data=res_data+ack[1]
		'''

		recv_packet=recv_one_message(soc)
		header,content=recv_packet.split(";;;")
		
		
	#soc.send("Thank You")
	#res=pickle.loads(res_data)
	print ("Closing socket")

	if header=="DONE_REDUCE":
		queue.append((outputpathname,start,end))
	else:
		queue.append((hostname,filepath,start,end,res_data))

	soc.close()



'''
filename="elements_cluster.txt"

create_server_list(filename)
#isp_dict(server_list)
hostname,portnumber=get_address('germanium.cs.pitt.edu',server_list)
print (hostname,portnumber)
#print (server_list)
'''

def main(filename):

	disp_dict(server_list)

	'''
	filename="elements_cluster.txt"
	create_server_list(filename)
	'''
		

	#filename="worker_cluster.txt"

	print ("Current Servers available")
	setup_initial_worker(filename)
	
	os.system("clear")
	disp_dict(worker_list)
	
		# next create a socket object 
	try :
		s= socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		print ("Socket created successfully")
	except:
		print ("Unable to set up the masterserver")
		sys.exit(1)  

	
	
	  
	# reserve a port on your computer in our 
	# case it is 12345 but it can be anything 
	#port = 12345           
	hostname,host_ip,port=get_address(socket.gethostname(),server_list)
	print (hostname,host_ip,port)
	
	# Next bind to the port 
	# we have not typed any ip in the ip field 
	# instead we have inputted an empty string 
	# this makes the server listen to requests  
	# coming from other computers on the network 

	s.bind((host_ip,int(port)))
	

	print "socket binded to %s" %(port) 
	
	print ("Socket name=",s.getsockname())
	#raw_input("")	  
	# put the socket into listening mode 
	s.listen(5)   
	#s2.listen(5)   
	print "socket is listening"            

	
	# a forever loop until we interrupt it or  
	# an error occurs 
	threads=[]
	while True: 
	  
	  # print ("Things are here")

	   # Establish connection with client. 
	   c, addr = s.accept()      

	   #print ('Got connection from', addr) 
	   
	   t=Thread(target=client_thread,args=[c,addr])
	   t.start()
	   #c.send('Thank you for connecting') 
	   #x=c.recv(1024)
	   threads.append(t)
	    
	#thread.start_new_thread(server_thread,(host2,port2,))
	s.close()

if __name__=="__main__":
	main(sys.argv[1])


