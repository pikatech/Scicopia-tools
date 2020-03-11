#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 13:55:23 2020

@author: kampe
"""

import itertools
import logging
from typing import List, Tuple

import spacy
import networkx as nx

NLP = spacy.load("en_core_web_lg", disable=["ner"])


def hearst(text: str) -> List[Tuple[str]]:
    """
    Extended information on the technique can be found in the original paper:

    Hearst, M. A.
    Automatic Acquisition of Hyponyms from Large Text Corpora
    COLING 1992 Volume 2: The 15th International Conference on Computational Linguistics, 1992
    https://www.aclweb.org/anthology/C92-2082

    Parameters
    ----------
    text : str
        Raw text.

    Returns
    -------
    List[Tuple[str]]
        A list of 3-tuples stating hyponymy relations, e.g. [('X', 'such as', 'Y')].

    """
    doc = NLP(text)
    edges = []
    for token in doc:
        for child in token.children:
            edges.append((token.i, child.i))

    graph = nx.Graph(edges)
    candidates = list(doc.noun_chunks)

    hits = []
    for source, target in itertools.combinations(candidates, 2):
        source_root = source.root.i
        target_root = target.root.i
        logging.debug("%d->%d:", source.root.i, target.root.i)
        path = nx.shortest_path(graph, source=source_root, target=target_root)
        logging.debug([doc[x].text for x in path])
        if (
            len(path) == 3
            and doc[path[1]].text == "as"
            and doc[path[1] - 1].text == "such"
        ):
            hits.append((source.lemma_, "such as", target.lemma_))
    return hits
