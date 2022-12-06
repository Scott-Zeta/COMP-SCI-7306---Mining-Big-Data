import argparse
import time
import random
import itertools

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', default="mushroom.dat")
parser.add_argument('-s', '--min_support', default = 0.5)
parser.add_argument('-p', '--probability', default = 0.5)               
args = parser.parse_args()

randomized_data = []
store_res = {}

def single_set(data_set, threshold):
    item_list = {}
    for line in data_set:
        tmp_list = set(line)
        for item in tmp_list:  
            if item not in item_list:
                item_list[item] = 1
            else:
                item_list[item] += 1
        freq_items = []
        for item in item_list:
            if item_list[item] >= threshold:
                freq_items.append(item)
    return freq_items

def a_priori(data, single, threshold, k):
    # exit recersion
    if store_res.get(k-1) is not None and len(store_res.get(k-1)) == 0:
        return
    
    valid_list = {}
    support_select = []
    
    for basket in data:
        for i in set(basket):
            if i not in single:
                basket.remove(i)
    # combination of item sets
    for basket in data:
        combination_lists = itertools.combinations(basket, k)
    
        for combination in combination_lists:
            if valid_list.get(combination) is None:
                valid_list[combination] = 1
            else:
                valid_list[combination] += 1

    for item in valid_list:
        if valid_list[item] > threshold:
            support_select.append(item)
    store_res[k] = support_select
    single = set()
    for i in support_select:
      for j in i:
        single.add(j)
    print("The frequent set with size %d:" % (k))
    print("count: %d" % ((len(support_select))))
    a_priori(data, single, threshold, k + 1)

def randomize(probability):
    return random.random() < float(probability)

def file_to_array():
    i = 1
    file = open(args.file, "r")
    
    for line in file:
        line = line.rstrip()
        line_row = line.split()
        
        if (randomize(args.probability)):
            randomized_data.append(line_row)
        i += 1
    file.close()
    print (i, " lines in total.")
    print("sample size = ", len(randomized_data))

def main():
    start_time = time.time()
    file_to_array()
    support = float(args.min_support)
    single = single_set(randomized_data, len(randomized_data) * support)
    store_res[1] = single
    print("The frequent set with size %d is:" % (1))
    print("the count is: %d" % ((len(single))))
    a_priori(randomized_data, single, len(randomized_data) * support, 2)
    print("Total running time: %s" % (time.time() - start_time))

if __name__ == "__main__":
    main()