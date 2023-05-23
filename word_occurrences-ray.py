from multiprocessing import Pool
from functools import reduce
from collections import Counter
from more_itertools import batched
import string
import time
import ray
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-w","--word", help="The word you want to count the occurrences of")
parser.add_argument("-fn","--filename", help="The file you want to count the occurrences of the word in")
parser.add_argument("-d","--devices", help="The number of devices into your ray cluster")
args = parser.parse_args()


def clean_word(word):
    return word.translate(str.maketrans('', '', string.punctuation))

def matches_word(word):
    if word == (args.word).lower():
        return word

def mapper(text):
    tokens_in_text = text.split(' ')
    tokens_in_text = map(clean_word, tokens_in_text)# A questo punto la porzione di testo presa in esame non ha segni di punteggiatura
    tokens_in_text=map(matches_word, tokens_in_text)# Qui invece controllo una per una se la parola è quella data in input
    return Counter(tokens_in_text)

def reducer(cnt1, cnt2):
    cnt1.update(cnt2)
    return cnt1

def chunk_mapper(chunk):
    mapped = map(mapper, chunk)
    reduced = reduce(reducer, mapped)
    return reduced


@ray.remote
def routine(data_chunks):
    data_chunks_gen = (y for y in data_chunks)
    #pool = Pool(8)
    mapped = map(chunk_mapper, data_chunks_gen)
    reduced = reduce(reducer, mapped)
    return reduced


#inspect_serializability(routine, name="routine")

ray.init()

#input("Dopo aver connesso tutti i dispositivi, premere un tasto per continuare...")

start=time.time()

with open(args.filename, "r") as dataf:
    data=dataf.read()

data=data.split(' ')
cluster=args.devices
data_chunks = batched(data, cluster)
data_chunks_gen = list(y for y in data_chunks)
#data_chunks_obj = ray.put(data_chunks_gen)

futures = [routine.remote(ray.put(data_chunks_gen[int(((len(data_chunks_gen)/cluster)*(i-1))):int(((len(data_chunks_gen)/cluster)*i))-1])) for i in range (1, cluster+1)]

ut=0
for i in range (0, len(ray.get(futures))):
    ut=ut+int(ray.get(futures)[i]['ut'])

print("'ut': ", ut)
print (time.time()-start)
