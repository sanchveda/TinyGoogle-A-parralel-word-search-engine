from collections import defaultdict
import os
import json
import string
import sys
import pickle

current_directory = os.getcwd()
def invertedindex_reducer(inputdirpath, start_alphabet, end_alphabet):
	index_dict = {}
	counts = {}
	words = []
	fileList = os.listdir(inputdirpath)
	exclude = set(string.punctuation)
	exclude1 = set({'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 'than'})
	
	os.chdir(inputdirpath)
	
	start_alphabet = start_alphabet.lower()
	end_alphabet = end_alphabet.lower()

	letters = string.ascii_lowercase
	allowed = letters[letters.index(start_alphabet):letters.index(end_alphabet)+1]  # abcdefg
			
	
	# for fileName1 in fileList:
		# input_file1 = open(fileName1,'rb')
		# index_dict_map1 = pickle.load(input_file1)
		# print(index_dict_map1)
		# print('\n\n')

	flag = 0
	for fileName1 in fileList:
		input_file1 = open(fileName1,'rb')
		index_dict_map1 = pickle.load(input_file1)
		for key1 in index_dict_map1:
			if key1[0] in  allowed:
				if key1 in index_dict.keys():
					val1 = index_dict_map1[key1]
					val2 = index_dict[key1]
					for x in val1:
						docname1 = x[0]
						flag = 0
						for index,y in enumerate(val2):
							docname2 = y[0]
							if docname1 == docname2:
								flag = 1	
								index_dict[key1][index] = (docname2,(int(x[1]) + int(y[1]))) 
						if flag == 0:
							index_dict[key1].append(x)
				else:
					index_dict[key1] = []
					index_dict[key1] = index_dict_map1[key1]
		input_file1.close()		
	
	os.chdir("../")
	return index_dict
	
	
				
if __name__ == '__main__':
	#invertedindex(inputdirpath, start_alphabet, end_alphabet)
	index_dict = invertedindex(sys.argv[1], sys.argv[2], sys.argv[3])	
	#index_dict = invertedindex("output", 'A', 'Z')
	print(index_dict)
	