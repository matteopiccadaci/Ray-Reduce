import multiprocessing
from multiprocessing import Pool
from functools import reduce
from collections import Counter
import re
from more_itertools import batched
import string
import time

def clean_word(word):
    return word.translate(str.maketrans('', '', string.punctuation))

def matches_word(word):
    if word == 'ut':
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

start=time.time()
#pool = Pool(8)
with open('../grande.txt', "r") as dataf:
    data=dataf.read()

data=data.split(' ')
data_chunks = batched(data,8)

mapped = map(chunk_mapper, data_chunks)

reduced = reduce(reducer, mapped)

print (reduced)
print(time.time()-start)