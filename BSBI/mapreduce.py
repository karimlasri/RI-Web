import os
import nltk
from nltk.stem.porter import *
from nltk.corpus import stopwords
from threading import Thread, Lock
from queue import Queue

doc_list = []
doc_queue = Queue()
terms_postings_dic = {}
terms_list = []
still_browsing = True

class MapReduce(Thread):
    def __init__(self, folder_path):
        Thread.__init__(self)
        self.__path = folder_path

    def run(self):
        browser = Docs_Browser(self.__path)
        browser.start()
        browser.join()
        mappers = []
        reducers = []
        for i in range(10):
            mappers.append(Mapper())
            reducers.append(Reducer())
        for i in range(10):
            mappers[i].start()
        for i in range(10):
            mappers[i].join()
        global terms_list
        terms_list = list(terms_postings_dic.keys())
        for i in range(10):
            reducers[i].start()
        for i in range(10):
            reducers[i].join()


class Docs_Browser(Thread):
    def __init__(self, path):
        Thread.__init__(self)
        self.__path = path
        self.__counter = 1

    def run(self):
        global doc_list
        global doc_queue
        for root, dirs, filenames in os.walk(self.__path, topdown = True):
            for filename in filenames:
                file_path = root + '/' + filename
                doc_list.append((self.__counter, file_path))
                doc_queue.put((self.__counter, file_path))
                self.__counter += 1
        global still_browsing
        still_browsing = False

class Mapper(Thread):
    counter = 1
    lock = Lock()
    def __init__(self):#, num):
        Thread.__init__(self)
        #self.number = num
        self.__stop_words = set(stopwords.words('english'))
        self.__stemmer = PorterStemmer()

    def map(self, doc_id, doc_text):
        # Dictionnaire de termes : fréquence pour un doc id
        dico = {}
        doc_tokens = re.split(r'[\s,.;?!\':()\[\]{}]+', doc_text)
        doc_tokens = [t.lower() for t in doc_tokens if t.lower() not in self.__stop_words]
        for token in doc_tokens:
            if token != '':
                term = self.__stemmer.stem(token)
                if term not in dico.keys():
                    dico[term] = 1
                else:
                    dico[term] += 1
        global terms_postings_dic
        with Mapper.lock:
            for term in dico.keys():
                if term in terms_postings_dic.keys():
                    terms_postings_dic[term] += [(doc_id, dico[term])]
                else:
                    terms_postings_dic[term] = [(doc_id, dico[term])]
                # add posting to a global list ?
                # emit(term, (doc_id, dico[term]))

    def run(self):
        global doc_queue
        global doc_list
        while still_browsing or not doc_queue.empty():
            if not doc_queue.empty():
                doc_id, filename = doc_queue.get()
                doc_text = ''
                with open(filename) as file:
                    doc_text = file.read()
                self.map(doc_id, doc_text)
                Mapper.counter += 1


class Reducer(Thread):
    lock = Lock()
    def __init__(self):
        Thread.__init__(self)

    def reduce(self, term, postings):
        posting_list = postings
        posting_list.sort(key=lambda x : x[0])
        terms_postings_dic[term] = posting_list
        return posting_list

    def run(self):
        global terms_list
        global terms_postings_dic
        with Reducer.lock:
            if terms_list != []:
                term = terms_list[0]
                terms_list.pop(0)
                terms_postings_dic[term] = self.reduce(term, terms_postings_dic[term])

# An auxiliary data structure is necessary to maintain the mapping from integer document ids to file names => list for example ?


mapreduce = MapReduce('./data/CS276/')
mapreduce.start()
mapreduce.join()

with open("mapreduce_index.txt", "wt") as index_file:
    for t in terms_postings_dic.keys():
        postings = [t + ',' + str(posting[0]) + ',' + str(posting[1]) for posting in terms_postings_dic[t]]
        line = ';'.join(postings) + '\n'
        index_file.write(line)
index_file.close()
