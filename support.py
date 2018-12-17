import pickle
import string
import io
import struct


def extract_filename(filepath):

	res=filepath.split('/')

	return res[len(res)-1]

def recv_one_message(sock):
    lengthbuf = recvall(sock, 4)
    length, = struct.unpack('!I', lengthbuf)
    return recvall(sock, length)

def recvall(sock, count):
    buf = b''
    while count:
        newbuf = sock.recv(count)
        if not newbuf: return None
        buf += newbuf
        count -= len(newbuf)
    return buf

def send_one_msg(sock,data):
	length=len(data)
	sock.sendall(struct.pack('!I',length))
	sock.sendall(data)


## Function to write the variable to the outputpathname
def write_file(outputpathname,variable):
	
	try :
		with open(outputpathname,'wb') as fp:
			pickle.dump(variable,fp,protocol=pickle.HIGHEST_PROTOCOL)
			return True
	except:
		print ("There is an issue with printing")
		return False
#####==============================================================

def firstletter(word):
	return string.upper(word[0])

def get_split(word,search_queue):

	start_letter=firstletter(word)
	#print (search_queue)


	for tups in search_queue:
		#print (tups)
		filename,start,end=tups
		#print (filename,start,end)

		if start_letter >= string.upper(start) and start_letter <= string.upper(end):
			return filename
	


def belongs_to(letter,letter_range):
	start,end=letter_range.split('-')

	if letter >= start and letter <=end:
		return 1
	else:
		return 0
#----------------------------------------------------#
def get_data():
	lines=[]

	with open ("Inputs/Inputs/war_and_peace.txt","r") as fp:
		line=fp.readline()
		while line:
			lines.append(line)
			line=fp.readline()

	data=pickle.dumps(lines)		

	
	return data
#---------------------------------------------



#------------------------Displaying a list-----------------------#
def disp_list(lst):
	for i in lst:
		print (i)
#----------------------------------------------------------------#


#=---------------Displaying a dictionary 
def disp_dict(d):
	for key ,values in d.items():
		print (key,values)

#------------------Function created for checking if the key exist in a list or not---------------------------#
def does_exist(key,target_list,value=False):

	if value==False:
		for target in target_list:
			if target==key:
				return True
	else:
		for target in target_list:
			x=target.split(";;;")[0]
			if x==key:
				return True
	return False

#--------------------------------------------#
# Takes in the hostname and returns the IP_address and portnumber
def  get_address(target,d):

	for key, values  in d.items():
	
		if key == target:
			hostname=key
			ip_address=values[0]
			portnumber=values[1]
			return hostname,ip_address,portnumber

	return "","",0
#-------------------------------------------#



#------------Same operation as the previous function except that here it searches by ip_address
def get_address_from_ip(target,d):

	for key, values in d.items():

		ip_key=values[0]

		if ip_key == target:
			hostname=key
			ip_address=values[0]
			portnumber=values[1]
			return hostname,ip_address,portnumber

	return "","",0
#-=========================================================================================#

#----------------------------------------#
def get_server_list():

	with open ('serverlist.p','r') as fp :
		server_list=pickle.load(fp)

	return server_list

if __name__=="__main__":
	d={}
	write_file("temp/asdasdasdasd.txt_splot",d)

#------------------------------------------------------------------------------------------------#
