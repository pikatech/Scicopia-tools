#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
import spacy
import pke
import string
from nltk.corpus import stopwords


class AutoTagger:
    field = "auto_tags"
    doc_section = "abstract"
    
    def __init__(self, model: str = "en_core_web_lg"):
        """
        Loads a spaCy model to be used with pke.

        Parameters
        ----------
        model : str
            The name of a spaCy model, e.g. "en_core_web_lg".

        Returns
        -------
        None.

        """
        self.nlp = spacy.load(model, disable=["ner", "textcat", "parser"])
        self.nlp.add_pipe(self.nlp.create_pipe("sentencizer"))

    def process(self, doc: str):
        """
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
            "auto_tags": List of keyphrases.

        """
        # Use a new MultiPartiteRank every time.
        # Trying to reuse one leads to a ZeroDivisionError: float division by zero
        extractor = pke.unsupervised.MultipartiteRank()
        extractor.load_document(input=doc, encoding="utf-8", spacy_model=self.nlp)
        pos = {"NOUN", "PROPN", "ADJ"}
        stoplist = list(string.punctuation)
        stoplist += ["-lrb-", "-rrb-", "-lcb-", "-rcb-", "-lsb-", "-rsb-"]
        stoplist += stopwords.words("english")
        extractor.candidate_selection(pos=pos, stoplist=stoplist)
        extractor.candidate_weighting(alpha=1.1, threshold=0.74, method="average")
        keyphrases = extractor.get_n_best(n=10)
        return {AutoTagger.field: [key[0] for key in keyphrases]}

    def release_resources(self):
        del self.nlp
