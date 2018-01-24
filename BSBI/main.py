import os
import nltk
from nltk.stem.porter import *
from nltk.corpus import stopwords

def parse_next_block(i, terms, docs, currentTermID, currentDocID):
    path = './data/CS276/' + str(i) + '/'
    stemmer = PorterStemmer()
    stop_words = set(stopwords.words('english'))
    list_pairs = []
    for filename in os.listdir(path):
        filepath = path + filename
        docs['/' + str(i) + '/' + filename] = currentDocID
        currentDocID += 1
        with open(filepath, 'r') as f:
            raw_doc = f.read()
            doc_tokens = re.split(r'[\s,.;?!\':()\[\]{}]+', raw_doc)
            doc_tokens = [t for t in doc_tokens if t not in stop_words]
            for token in doc_tokens:
                if token != '':
                    term = stemmer.stem(token.lower()) # stemming token
                    if term not in terms.keys():
                        terms[term] = currentTermID
                        currentTermID += 1
                    list_pairs += [(terms[term], currentDocID)]
    return list_pairs, terms, docs, currentTermID, currentDocID


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
            file.write('%s\n' % ';'.join([','.join([str(i) for i in posting]) for posting in word_postings]))


def merge_blocks():
    files = [open('%s_postings.txt' % i, 'r', 1024*1024) for i in range(10)]
        #open('0_postings'), open('1_postings'), open('2_postings'), open('3_postings'), open('4_postings'), open('5_postings'), open('6_postings'), open('7_postings'), open('8_postings'), open('9_postings')
    all_empty = False
    empty = []
    buffers = [[] for i in range(10)]
    for i in range(10):
        line = files[i].readline()
        if line == '':
            empty += [i]
            buffers[i] = []
        else:
            # print(line)
            # print(line.split(';'))
            buffers[i] = [posting.split(',') for posting in line[:len(line)-1].split(';')]
            #print(buffers[i])

    cur_term_id = 0
    list_cur_term = []
    with open('merged_postings.txt', 'wt') as out:
        while not(all_empty):
            for i in range(10):
                if buffers[i] == [] and i not in empty:
                    line = files[i].readline()
                    print(line)
                    if line == '':
                        empty += [i]
                        buffers[i] = []
                    else:
                        buffers[i] = [posting.split(',') for posting in line[:len(line)-1].split(';')]
                if buffers[i] != []:
                    if buffers[i][0][0] == str(cur_term_id):
                        list_cur_term += buffers[i]
                        buffers[i] = []
                        print(cur_term_id)
            cur_term_id += 1
            out.write(';'.join([','.join([str(i) for i in posting]) for posting in list_cur_term])+'\n')
            list_cur_term = []
            all_empty = (len(empty) == len(files))
    out.close()


def bsbi_index_construction():
    termDic = {}
    docDic = {}
    curTermID = 0
    curDocID = 0
    for i in range(10):
        print("Parsing block {}.".format(str(i)))
        list_pairs, termDic, docDic, curTermID, curDocID = parse_next_block(i, termDic, docDic, curTermID, curDocID)
        print("Inverting pairs for block {}.".format(str(i)))
        list_postings = bsbi_invert(list_pairs)
        print("Writing data for block {} to disk.".format(str(i)))
        write_block_to_disk(list_postings, i)
    merge_blocks()


#bsbi_index_construction()
merge_blocks()

