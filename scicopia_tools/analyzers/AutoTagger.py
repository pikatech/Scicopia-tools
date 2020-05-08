#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
import pke
import string
from nltk.corpus import stopwords

class AutoTagger:
    
    def __init__(self, model: str="en_core_web_lg"):
        '''
        Loads a spaCy model.

        Parameters
        ----------
        model : str
            The name of a spaCy model, e.g. "en_core_web_lg".

        Returns
        -------
        None.

        '''
        self.extractor = pke.unsupervised.MultipartiteRank()


    def process(self, doc: str):
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
        dict
            DESCRIPTION.

        '''
        self.extractor.load_document(input=doc, encoding="utf-8")
        pos = {'NOUN', 'PROPN', 'ADJ'}
        stoplist = list(string.punctuation)
        stoplist += ['-lrb-', '-rrb-', '-lcb-', '-rcb-', '-lsb-', '-rsb-']
        stoplist += stopwords.words('english')
        self.extractor.candidate_selection(pos=pos, stoplist=stoplist)
        self.extractor.candidate_weighting(alpha=1.1,
                                  threshold=0.74,
                                  method='average')
        keyphrases = self.extractor.get_n_best(n=10)
        return {'auto_tags':[key[0] for key in keyphrases]}
