#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
from scicopia_tools.analyzers import Analyzer
import spacy


class TextSplitter(Analyzer):
    field = "abstract_offsets"
    doc_section = "abstract"

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
        super().__init__()
        self.nlp = spacy.load(
            model,
            exclude=[
                "ner",
                "textcat",
                "parser",
                "lemmatizer",
                "tagger",
                "attribute_ruler",
            ],
        )
        self.nlp.enable_pipe("senter")

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
        return {TextSplitter.field: [(x.start_char, x.end_char) for x in doc.sents]}

    def release_resources(self):
        del self.nlp
