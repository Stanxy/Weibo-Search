import os
import sys
import json
import time
import logging
import jieba
import jieba.analyse
import jieba.posseg as pseg
import operator
import numpy as np
from gensim.models import word2vec
from gensim.models import KeyedVectors

class Word2Vec:
    """
    The Word2Vec class, building a word to vec instance. the path of the w2v .bin file is needed when initialize.
    """
    def __init__(self, format_file_path):
        self.model = self.load_model(format_file_path)

    def load_model(self, path):
        try:
            model = KeyedVectors.load_word2vec_format(path, binary=False)
        except:
            pass
        return model

    def gen_word_vec(self,word):
        try:
            if word in self.model:
                vector = self.model[word]
            else:
                vector = np.zeros(100)
            str_vector = ' '.join(str(det) for det in list(vector))
        except:
            pass
        return str_vector

def embedding_config(word_to_vec_path, stop_word_path, dict_path_list):
    """
    Set all the configuration for jieba and gensim.
    
    Arguments:
    word_to_vec_path -- the path of the word_to_vec bin file, dtype: string
    stop_word_path -- the path of the stop word dictionary, dtype: string
    dict_path_list -- the list of all the path of the dictionarys needed, dtype: string
    
    Returns:
    Succeed -- whether you have successfully set the jieba
    v2w -- the instance of word_2_vec
    """
    
    # Load in all the dictionary
    try:
        for path in dict_path_list:
            jieba.load_userdict(path)
            print('{} has been successfully loaded!'.format(path))
        jieba.initialize()
        flag1 = True
    except:
        print('Check your dictionary path: {}!'.format(path))
    
    
    # Set the stop words
    try:
        jieba.analyse.set_stop_words(stop_word_path)
        print("Successfully set the stopwords list!")
        flag2 = True
    except:
        print("Check your stopwords path!")
        
    
    # Set the word to vec instance'
    try:
        w2v = Word2Vec(word_to_vec_path)
        print("Successfully initialize the word2vec instance!")
        flag3 = True
    except:
        print("Check your word2vec path!")
    
    Flag = flag1 and flag2 and flag3
    return w2v, Flag

def phrase_embedding(S, w2v, top_k_importance = 5 ,filter = False, star_pool = None, event_pool = None):
    """
    Embedding a phrase or word into a lower dimension space. The output represents the representation of the feature of the whole 
    sentence instead of a single word.
    
    Arguments:
    S -- the whole pharse, dtype: string
    w2v -- object mapping words to their BERT vector representation, dtype: object
    top_k_importance -- the top k words analyzed by jieba after removing stopwords and applying tf-idf filter
    star_pool -- star name set that used to tract stars from the tags, dtype: list( Preparing )
    event_pool -- event that used to tract events from the tags, dtype: list( Preparing )
    
    Returns:
    embedding_vec -- phrase embedding vector, of shape(ndim, )
    """
    
    word_tags = jieba.analyse.extract_tags(S, top_k_importance)           # substract the critical tags that we need
    
    if filter:
    # We presume that the start names is the priority users care, thus we retrive all the star names from the word bag
        if len(list(set(word_tags) & set(star_pool))):
            effective_word_tags = list(set(word_tags) & set(star_pool))

        # If that strategy doesn't work, then we substract the event words
        else:
            word_tags = jieba.analyse.extract_tags(S, 20)
            effective_word_tags = list(set(word_tags) & set(event_pool))


    effective_word_tags = word_tags
    
    # Use the word we have retrived, we need to transform them into vectors
    vec_stack = []
    for word in effective_word_tags:
        str_vector = w2v.gen_word_vec(word)
        str_vec = np.array([float(i) for i in str_vector.split(" ")]).reshape(-1,1)
        vec_stack.append(str_vec)
    
    # We here simple average all these vectors, we may choose to adopt a strategy to adapt this later 
    ave_vec = np.zeros(str_vec.shape)
    for vec in vec_stack:
        ave_vec += vec
    ave_vec = ave_vec/len(vec_stack)        

    return ave_vec