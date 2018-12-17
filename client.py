
import socket
import sys
import os
import pickle
import numpy as np
import string
import glob
from support import *
import time

from SearchQuery import *

server_list=get_server_list()
global count
count=0

##----------The below functions are very specific to the client and that's why it is mentioned here----------------##
def disp_menu():
    
    os.system("clear")
    print ("***********************WELCOME USER at %s**********************" %(socket.gethostname()) )
    print ("============================================================================")
    print ("INDEX")
    print ("SEARCH")

def read_search_input():

    while True:
        line=raw_input("Please enter your search keywords separated by a space===>")

        words=line.split()

        if not any(words):
            print ("You have entered nothing")
        else:
            break    

  
    print ("These are the words that you have entered\n\n",words)
    return words        

##-----------------------------------------------------------------------------------------##

def main():
   
    global count
    ##### The next try catch sections are trying to establish the connection with the masterserver----#######   
    try :
	   soc=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	   #rint ("Socket Sucessfully created")
    except socet.error as err:
	   print ("Socket creation failed with error %s" %(err))



    try:
    	host = socket.gethostbyname('neptunium.cs.pitt.edu')
    	hostname,host_ip,port=get_address_from_ip(host,server_list)
	
    except socket.gaierror:
    	#This could not resolve the host
    	print ("There was an error resolving the host")
    	sys.exit()


    try:
        soc.connect((host_ip, int(port)))
    except:
        print("Connection error")
        sys.exit()
    #####--------------------------------End of connection setups with the masterserver ----------------#######
    
    disp_menu()
    message = string.upper(raw_input("Enter your message(Enter EXIT to end)-> "))

    while message != "EXIT":
        #soc.sendall(message.encode("utf8"))


        if string.upper(message) == "INDEX": ## if it is a indexing request
            dir_path=raw_input("\nEnter filepathlist:")
            filepathlist=glob.glob(dir_path+"/*")
            #filepathlist=("filename1.txt","filename2.txt")
            #message_content=filepathlist
            
            p=pickle.dumps(filepathlist)

            data=string.upper(message)+";;;"+p+";;;"+str(count)
            start_time=time.time()
            send_one_msg(soc,data)
            #tup=(message,message_content,str(count))
            #send_packet=pickle.dumps(tup)
            #soc.send(send_packet)
            
            count=count+1 #Global count is increased by 1 everytime an indexing is done
            
            rec_packet=recv_one_message(soc)
            end_time=time.time()
            recv_msg=rec_packet.split(';;;')
            #recv_msg=pickle.loads(rec_packet)
            print ("Received message",recv_msg)
            print ("Time taken =%s" %(end_time-start_time))
    	    
        elif string.upper(message) == "SEARCH": ## if it is a searching request
            
            searchwordlist=read_search_input()
            #searchwordlist=("word1","word2","word3")
            
            #---------Sending the data---------------#
            p=pickle.dumps(searchwordlist)
            start_com_time=time.time()
            data=string.upper(message)+";;;"+p
            send_one_msg(soc,data)
            
            x=None
            #---------Receiving hte results from the server -------#
            rec_packet=recv_one_message(soc)
            end_com_time=time.time()
            header,content=rec_packet.split(";;;")

            print ("\n******Results section*******")
            print ("-------------------------------")
          
            while header != "SEARCH_COMPLETE":
                data=pickle.loads(content)
                
                if data[1] != None:
                    print (data[0],data[1])
                    x=search(x,data[1])
                    #print (data[1][0])
                    #print (len(data[1]))
                else:
                    print ("No word has been found for  \'%s\'" %(data[0]))

                rec_packet=recv_one_message(soc)
                header,content=rec_packet.split(";;;")

            if header == "SEARCH_COMPLETE":
                rec_msg=rec_packet.split(";;;")
                print ("\nSearch is done\n")
            else:
                print ("There was an error")
            
            print ("\n")
            print ("Result=",x)
            print ("\nTime taken= %s" %(end_com_time-start_com_time))
            print ("Time taken for process  = %s" %(content))
        else:
            print("\nYou have entered a wrong input.")

        raw_input("\n Enter any key to continue")

        disp_menu()
        message=string.upper(raw_input("Enter your message(Enter EXIT to end->"))

        

    #tup=("EXIT","The client will close it's socket now. The client is satisfied")
    data="EXIT"+";;;"+"The client will close it's socket now. The client is satisfied"
    send_one_msg(soc,data)
    #send_packet=pickle.dumps(tup)
    #soc.send(send_packet)
    
    print ("This client will close it's socket from the client side")
    soc.close()

if __name__ == "__main__":
    main()
