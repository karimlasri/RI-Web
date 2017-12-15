import os
import nltk
from nltk.stem.porter import *


def parse_next_block(i, terms, docs, currentTermID, currentDocID):
    path = './data/CS276/' + str(i) + '/'
    stemmer = PorterStemmer()
    list_pairs = []
    for filename in os.listdir(path):
        filepath = path + filename
        docs['/' + str(i) + '/' + filename] = currentDocID
        currentDocID += 1
        with open(filepath, 'r') as f:
            raw_doc = f.read()
            doc_tokens = re.split(r'[\s,.;?!\':()\[\]{}]+', raw_doc)
            for token in doc_tokens:
                if token != '':
                    term = stemmer.stem(token.lower()) # stemming token
                    if term not in terms.keys():
                        terms[term] = currentTermID
                        currentTermID += 1
                    list_pairs += [(terms[term], currentDocID)]
    return list_pairs


def bsbi_invert(list_of_pairs):
    list_of_pairs.sort(key = lambda x : x[0])
    list_postings_all = []
    for e1, e2 in list_of_pairs:
        if len(list_postings_all) == 0:
            list_postings_all += [[[e1, e2, 1]]]
        else:
            list_posting_last_word = list_postings_all[len(list_postings_all)-1]
            last_posting = list_posting_last_word[len(list_posting_last_word)-1]
            if e1 == last_posting[0]:
                if e2 == last_posting[1]:
                    last_posting[2] += 1
                else:
                    list_posting_last_word += [[e1, e2, 1]]
            else:
                list_postings_all += [[[e1, e2, 1]]]
    return list_postings_all


def write_block_to_disk(list_of_postings, i):
    with open(str(i)+'_postings.txt', 'wt') as file:
        for word_postings in list_of_postings:
            file.write('%s\n' % word_postings)
    return(i+1)

# def merge_blocks():
#     files = open('0_postings'), open('1_postings'), open('2_postings'), open('3_postings'), open('4_postings'), open('5_postings'), open('6_postings'), open('7_postings'), open('8_postings'), open('9_postings')
#     with open('merged_postings', 'wt') as out:
#         out.writelines()

def bsbi_index_construction():
    n = 0
    termDic = {}
    docDic = {}
    curTermID = 0
    curDocID = 0
    for i in range(10):
        print("Parsing block {}.".format(str(n)))
        list_pairs = parse_next_block(n, termDic, docDic, curTermID, curDocID)
        print("Inverting pairs for block {}.".format(str(n)))
        list_postings = bsbi_invert(list_pairs)
        print("Writing data for block {} to disk.".format(str(n)))
        n = write_block_to_disk(list_postings, n)
    #merge_blocks()


bsbi_index_construction()

