import multiprocessing
from multiprocessing import Pool
from functools import reduce
from collections import Counter
from more_itertools import batched
import string
import time
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-w","--word", help="The word you want to count the occurrences of")
parser.add_argument("-fn","--filename", help="The file you want to count the occurrences of the word in")
args = parser.parse_args()


def clean_word(word):
    return word.translate(str.maketrans('', '', string.punctuation)).lower()

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

if __name__ == '__main__':
    start=time.time()

    pool = Pool(8)
    with open(args.filename, "r") as dataf:
        data=dataf.read()

    data=data.split(' ')
    data_chunks = batched(data,multiprocessing.cpu_count())
    mapped = pool.map(chunk_mapper, data_chunks)

    reduced = reduce(reducer, mapped)

    print (reduced)
    print (time.time()-start)
