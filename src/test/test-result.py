#!/usr/bin/python
import sys
import os.path

# Check cli arguments
args = sys.argv
if (len(args) < 3):
    sys.exit("ERROR: Usage: python test-result.py <path to file> <path to mr results directory>")

# Ensure input file exists
input_file_path = args[1]
if not os.path.isfile(input_file_path):
    sys.exit("ERROR: {} is not found".format(input_file_path))

# Ensure map reduce results exits
result_dir_path = args[2]
result_file_list = os.listdir(result_dir_path)
if len(result_file_list) == 0:
    sys.ext("ERROR: directory {} is empty, did you forget to run map reduce?".format(result_dir_path))

# Count words in input file
word_counts = {}
with open(input_file_path, 'r') as f:
    for line in f:
        for word in line.split():
            if word not in word_counts:
                word_counts[word] = 1
            else:
                word_counts[word] += 1

# Get resulting key value pairs for output file
mr_word_counts = {}
for res_file in result_file_list:
    with open(result_dir_path + res_file, 'r') as f:
        for line in f:
            pair = line.split()
            if len(pair) != 2:
                continue
            key = pair[0]
            val = int(pair[1])
            mr_word_counts[key] = val

# Check word counts
print "INFO: Checking word counts match . . ."
success = (mr_word_counts == word_counts)
if success:
    print "INFO: PASS"
else:
    print "INFO: FAILED - Result does not match expected word count"
    for word in word_counts:
        if word not in mr_word_counts:
            print "\"{}\" \t\t\t does not exist in mr_word_counts, should exist with count of {}".format(word, word_counts[word])
        elif word_counts[word] != mr_word_counts[word]:
            if len(word) >= 5:
                tab_string = "\t"
            else:
                tab_string = "\t\t"
            print "\"{}\" {} found {} times in MR, should be {}".format(word, tab_string, word_counts[word], mr_word_counts[word])
    for word in mr_word_counts:
        if word not in word_counts:
            print "\"{}\" \t\t\t should not exist in MR".format(word)

# Check result files are ordered correctly
print "INFO: Checking result files are ordered . . ."
sorted_correctly = True
for res_file in result_file_list:
    with open(result_dir_path + res_file, 'r') as f:
        prev_key = None
        for line in f:
            pair = line.split()
            if len(pair) != 2:
                continue
            key = pair[0]
            if key < prev_key:
                print "INFO: FAILED - File {} not ordered correctly on key \"{}\"".format(res_file, key)
                sorted_correctly = False
            prev_key = key

if sorted_correctly:
    print "INFO: PASS"
