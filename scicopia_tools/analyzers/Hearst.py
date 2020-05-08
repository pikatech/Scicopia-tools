#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 13:55:23 2020

@author: kampe
"""

from functools import cmp_to_key
import itertools
import logging
from typing import List, Tuple

import spacy
from spacy.tokens.span import Span
import networkx as nx

NLP = spacy.load("en_core_web_lg", disable=["ner"])


def interval_sort(entity1, entity2) -> int:
    if entity1[0] < entity2[0]:
        return -1
    if entity1[0] > entity2[0]:
        return 1
    return entity2[1] - entity1[1]


IntervalKey = cmp_to_key(interval_sort)


def conflate_conjuncts(chunks: List[Span]) -> List[List[Span]]:
    intervals = []
    for c in chunks:
        parts = c.conjuncts
        intervals.append((min(min(map(lambda x: x.i, parts)), c.start), max(max(map(lambda x: x.i, parts)), c.end-1), c) if parts else (c.start, c.end - 1, c))

    intervals.sort(key=IntervalKey)
    filtered = []
    start = -1
    end = -1
    for interval in intervals:
        if interval[0] >= start and interval[1] <= end:
            filtered[len(filtered)-1].append(interval[2])
            continue
        filtered.append([interval[2]])
        start = interval[0]
        end = interval[1]
    return filtered


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
    candidates = conflate_conjuncts(candidates)

    hits = []
    for source, target in itertools.combinations(candidates, 2):
        source_root = source[0].root.i
        target_root = target[0].root.i
        logging.debug("%d->%d:", source[0].root.i, target[0].root.i)
        try:
            path = nx.shortest_path(graph, source=source_root, target=target_root)
            logging.debug([doc[x].text for x in path])
            if (
                len(path) == 3
                and doc[path[1]].text == "as"
                and doc[path[1] - 1].text == "such"
            ):
                for t in target:
                    hits.append((source[0].lemma_, "such as", t.lemma_))
        except nx.NetworkXNoPath:
            pass
    return hits
