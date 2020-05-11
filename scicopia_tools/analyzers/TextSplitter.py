#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
import spacy


class TextSplitter:
    def __init__(self, model: str = "en_core_web_lg"):
        """
        Loads a spaCy model.

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

    def process(self, text: str):
        """
        Splits the given text in sentences and 
        returns a list of tuples of start and end positions of each sentence.

        Parameters
        ----------
        text : str
            Text to split.

        Returns
        -------
        list
            List of tuples of start and end positions of each sentence

        """
        doc = self.nlp(text)
        return {"abstract_offsets": [(x.start_char, x.end_char) for x in doc.sents]}
