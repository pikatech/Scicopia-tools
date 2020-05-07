#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 13:44:30 2020

@author: tech
"""
import pke

def splitter(doc: str):
    '''
    Splitts the given text in sentences and 
    return list with tuple of start and end off each sentence.

    Parameters
    ----------
    doc : str
        Text to split.

    Returns
    -------
    abstract_offset : list
        list with tuple of start and end of each sentence

    '''
    extractor = pke.unsupervised.MultipartiteRank()
    extractor.load_document(input=doc, encoding="utf-8")
    sentences = extractor.sentences
    meta = [sentence.meta for sentence in sentences]
    abstract_offset = []
    for offset in meta:
        abstract_offset.append((offset["char_offsets"][0][0], offset["char_offsets"][-1][-1]))

    return {"abstract_offset":abstract_offset}
