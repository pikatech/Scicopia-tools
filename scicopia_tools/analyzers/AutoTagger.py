#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
from scicopia_tools.analyzers import Analyzer
import string

import pke
import spacy
from nltk.corpus import stopwords


class AutoTagger(Analyzer):
    field = "tags"
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
        super().__init__()
        self.nlp = spacy.load(model, exclude=["ner", "textcat", "parser"])
        self.nlp.enable_pipe("senter")

    def process(self, text: str):
        """
        Performs MultipartiteRank keyphrase extraction via pke
        (Python Keyphrase Extraction toolkit).
        Details are given in https://arxiv.org/abs/1803.0872 and
        https://boudinfl.github.io/pke/build/html/unsupervised.html#multipartiterank

        Parameters
        ----------
        text : str
            Text to extract keyphrases from.

        Returns
        -------
        dict
            "tags": List of keyphrases.

        """
        # Use a new MultiPartiteRank every time.
        # Trying to reuse one leads to a ZeroDivisionError: float division by zero
        extractor = pke.unsupervised.MultipartiteRank()
        extractor.load_document(input=text, encoding="utf-8", spacy_model=self.nlp)
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
