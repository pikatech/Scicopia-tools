#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
import pke
import string
from nltk.corpus import stopwords

def auto_tag(doc: str):
    '''
    Performs MultipartiteRank keyphrase extraction via pke
    (Python Keyphrase Extraction toolkit).
    Details are given in https://arxiv.org/abs/1803.0872 and
    https://boudinfl.github.io/pke/build/html/unsupervised.html#multipartiterank

    Parameters
    ----------
    doc : str
        Text to extract keyphrases from.

    Returns
    -------
    list
        DESCRIPTION.
    words : TYPE
        DESCRIPTION.
    meta : TYPE
        DESCRIPTION.

    '''
    extractor = pke.unsupervised.MultipartiteRank()
    extractor.load_document(input=doc, encoding="utf-8")
    sentences = extractor.sentences
    words = [sentence.words for sentence in sentences]
    meta = [sentence.meta for sentence in sentences]
    pos = {'NOUN', 'PROPN', 'ADJ'}
    stoplist = list(string.punctuation)
    stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
    stoplist += stopwords.words('english')
    extractor.candidate_selection(pos=pos, stoplist=stoplist)
    extractor.candidate_weighting(alpha=1.1,
                              threshold=0.74,
                              method='average')
    keyphrases = extractor.get_n_best(n=10)
    entrys = {'auto_tags':[key[0] for key in keyphrases],'words':words,'meta':meta}
    return entrys
