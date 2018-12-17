from collections import defaultdict
import os
import json
import string
import sys

def invertedindex_mapper(inputfile, start_index, end_index):
	index_dict = {}

	exclude = set(string.punctuation)
	exclude1 = set({'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than'})
	#for fileName in fileList:

	'''
	if os.path.exists(inputfile):
		print ("inputfile exists",inputfile)
		print ("The current working directory",os.getcwd())
	else:
		print ("It does not exist",inputfile)
		print ("The current working directory",os.getcwd())
	'''
	with open(inputfile,'r') as f:
		#print(f.read())
		data = f.readlines()[start_index:end_index+1]
		datastring = ''.join(data)

		a1 = ''.join([i for i in datastring.lower() if not i.isdigit()])
		a2 = ''.join([i for i in a1 if i not in exclude])
		
		words = a2.lower().split()
		counts = {}
		for word in words:
			if word in counts.keys():
				counts[word] += 1
			else:
				counts[word] = 1
		#print( counts )

		words = list(set(words))
		
		for word in words:
			if word not in index_dict.keys():
				index_dict[word] = {}
				index_dict[word] = []
			index_dict[word]+= [(inputfile, counts[word])]
		f.close()
		
	return index_dict
	
if __name__ == '__main__':
	#inputfile = sys.argv[1]
	#invertedindex(inputfile, start-index, end-index)
	##invertedindex('input/doc2.txt', 0, 4)
	index_dict = invertedindex(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
	#print(index_dict)