#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 13:55:23 2020

@author: kampe
"""

import itertools
import logging
from functools import cmp_to_key
from typing import List, Tuple

import networkx as nx
import spacy
from spacy.tokens.span import Span


class Hearst:
    field = "hearst"
    doc_section = "abstract"

    def __init__(self, model: str = "en_core_web_lg", extra=[]):
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
        self.nlp = spacy.load(model, exclude=["ner", "textcat"])
        for add_me in extra:
            if "component" in add_me and "config" in add_me:
                self.nlp.add_pipe(add_me["component"], config=add_me["config"])

        self.IntervalKey = cmp_to_key(self.interval_sort)

    def interval_sort(self, entity1, entity2) -> int:
        if entity1[0] < entity2[0]:
            return -1
        if entity1[0] > entity2[0]:
            return 1
        return entity2[1] - entity1[1]

    def conflate_conjuncts(self, chunks: List[Span]) -> List[List[Span]]:
        intervals = []
        for c in chunks:
            parts = c.conjuncts
            intervals.append(
                (
                    min(min(map(lambda x: x.i, parts)), c.start),
                    max(max(map(lambda x: x.i, parts)), c.end - 1),
                    c,
                )
                if parts
                else (c.start, c.end - 1, c)
            )

        intervals.sort(key=self.IntervalKey)
        filtered = []
        start = -1
        end = -1
        for interval in intervals:
            if interval[0] >= start and interval[1] <= end:
                filtered[len(filtered) - 1].append(interval[2])
                continue
            filtered.append([interval[2]])
            start = interval[0]
            end = interval[1]
        return filtered

    def process(self, text: str) -> List[Tuple[str]]:
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
        doc = self.nlp(text)
        hits = []

        for sent in doc.sents:
            edges = []
            for token in sent:
                for child in token.children:
                    edges.append((token.i, child.i))

            graph = nx.Graph(edges)
            candidates = list(sent.noun_chunks)
            candidates = self.conflate_conjuncts(candidates)
            print(f"candidates: {candidates}")

            for source, target in itertools.combinations(candidates, 2):
                source_root = source[0].root.i
                target_root = target[0].root.i
                logging.debug("%d->%d:", source[0].root.i, target[0].root.i)
                try:
                    path = nx.shortest_path(
                        graph, source=source_root, target=target_root
                    )
                    logging.debug([doc[x].text for x in path])
                    if (
                        len(path) == 3
                        and doc[path[1]].text == "as"
                        and doc[path[1] - 1].text == "such"
                    ):
                        span1 = source[0]
                        if (
                            doc[span1.start].pos_ == "DET"
                            or doc[span1.start].pos_ == "PUNCT"
                        ):
                            span1 = Span(doc, span1.start + 1, span1.end)
                        for t in target:
                            span2 = t
                            if doc[span2.start].pos_ == "DET":
                                span2 = Span(doc, span2.start + 1, span2.end)
                            hits.append((span1.text, "such as", span2.text))
                except nx.NetworkXNoPath:
                    pass
        print(hits)
        return {Hearst.field: hits}

    def release_resources(self):
        del self.nlp
