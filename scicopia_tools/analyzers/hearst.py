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


def hearst(text: str) -> List[Tuple[str]]:
    doc = nlp(text)
    edges = []
    for token in doc:
        for child in token.children:
            edges.append((token.i, child.i))

    graph = nx.Graph(edges)
    candidates = [chunk for chunk in doc.noun_chunks]

    hits = []
    for source, target in itertools.combinations(candidates, 2):
        source_root = source.root.i
        target_root = target.root.i
        logging.debug(f"{source}->{target}:")
        path = nx.shortest_path(graph, source=source_root, target=target_root)
        logging.debug([doc[x].text for x in path])
        if (
            len(path) == 3
            and doc[path[1]].text == "as"
            and doc[path[1] - 1].text == "such"
        ):
            hits.append((source.lemma_, "such as", target.lemma_))
    return hits
