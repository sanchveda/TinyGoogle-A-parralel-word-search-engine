import pickle
import string
import sys

def SearchQuery(fileName, keyword):

	input_file = open(fileName,'rb')
	index_dict = pickle.load(input_file)
	search_dict = {}
	docs_list = {}

	if keyword in index_dict.keys():
		#print(index_dict[keyword])
		for docs in index_dict[keyword]:
			docs_list[docs[0]] = docs[1]
	else:
		print("not found")
		return ""
	
	search_dict[keyword] = sorted(docs_list, key=docs_list.__getitem__, reverse=True)
	return search_dict	

def searchword(filename,keyword):

	print ("INSIDE SEARCH WORD",filename,keyword)

	with open (filename,"rb") as fp:
		index_dict=pickle.load(fp)


	for keys,values in index_dict.items():

		if keys==string.lower(keyword):
			tup=(keys,values)
			r_val=sorted(tup[1], key=lambda x:x[1])
			r_val.reverse()
			return r_val

	
	return "" 

def search(oldlist, newlist):

	reslist=[]

	flag=False
	if oldlist==None:
		reslist=newlist
	else:

		reslist=oldlist
		for items in newlist:
			flag=False
			for j in range(len(reslist)):
				#print (items[0],oldlist[j][0])

				if items[0]==oldlist[j][0]:
					flag=True
					newvalue=items[1]+oldlist[j][1]
					reslist[j]=((items[0],newvalue))
			
			if not flag:
				reslist.append(items)

	r_val=sorted(reslist, key=lambda x:x[1])
	r_val.reverse()

	#print (r_val)

	return r_val

if __name__ == '__main__':
	search_dict = SearchQuery(sys.argv[1], sys.argv[2])
	print(search_dict)