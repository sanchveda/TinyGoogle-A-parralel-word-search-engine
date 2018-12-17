import sys
import string

def split_InvertedIndex_reducer(no_splits):
	letters = string.ascii_lowercase
	#print(letters)
	total_letters = 26
	out_list = []
	letters_per_split = total_letters/no_splits
	remainder = total_letters % no_splits
	i = 0
	while(i<total_letters):
		if(len(out_list) == no_splits - 1):
			letters_per_split = letters_per_split + remainder
		out_list.append((letters[i],letters[i+letters_per_split-1]))
		i = i + letters_per_split

	return out_list

if __name__ == '__main__':
	out_list = split_InvertedIndex_reducer(int(sys.argv[1]))
	#print(out_list)
