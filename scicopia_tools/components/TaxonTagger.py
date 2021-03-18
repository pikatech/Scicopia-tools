#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 11:21:10 2020

@author: tech
"""

import logging
import re  # Only used in exception handling
from collections import namedtuple
from functools import cmp_to_key
from typing import List

from ahocorasick import Automaton
from intervaltree import IntervalTree
from spacy.tokens import Span

Annotation = namedtuple("Annotation", ["name", "label", "start", "end"])

NEGATIVE_TAX = set(
    [
        "other",
        "not shown",
        "This",
        "e.g.",
        "e.g",
        "plasmids",
        "all",
        "unclassified",
        "unclassified viruses",
        "unknown",
        "Data",
        "none",
        "root",
        "mixed culture",
        "hybrid",
        "ray",
        "vectors",
        "seal",
        "insertion sequence",
        "permit",
        "artificial",
        "collection",
        "spot",
        "metagenome",
        "Patricia",
        "e. a",
        "e. e",
        "name",
        "synthetic",
    ]
)


class TaxonTagger:

    label = "TAXON"
    name = "taxon_tagger"

    def __init__(self, wordlist, finalize: bool = True):
        """
        Creates an Aho-Corasick automaton out of a text file.
        
        :param wordlist: An open file with one entity per line.
        
        """
        automaton = Automaton()
        lifeforms = dict()
        for line in wordlist:
            id_taxons = line.rstrip().split("\t")
            if len(id_taxons) != 2:
                logging.warning("Found line with wrong format: %s", line)
                continue
            delim = id_taxons[0].rindex(":")
            label = id_taxons[0][delim + 1 :]
            variants = id_taxons[1].split("|")
            for v in variants:
                if v in NEGATIVE_TAX:
                    continue
                if v in lifeforms:
                    dictvalue = lifeforms[v]
                    if isinstance(dictvalue, str):
                        dictvalue = tuple([dictvalue, label])
                    else:  # tuple
                        dictvalue = list(dictvalue)
                        dictvalue.append(label)
                        dictvalue = tuple(dictvalue)
                    lifeforms[v] = dictvalue
                else:
                    lifeforms[v] = (label,)

        for key in lifeforms:
            if key != "":
                _ = automaton.add_word(key, (key, lifeforms[key]))

        if finalize:
            automaton.make_automaton()
        self.automaton = automaton
        self.EntityKey = cmp_to_key(TaxonTagger.entity_sort)
        if not Span.get_extension("id_candidates"):
            Span.set_extension("id_candidates", default=object())

    def __call__(self, doc):
        """
        Applies the tagger automaton to the text.
    
        Parameters
        ----------
        text : str
            The text we want to search for entities
        tagger : Automaton
            An instance of an Aho-Corasick automaton
        
        """
        text = doc.text
        annotations = []
        for end, (key, labels) in self.automaton.iter(text):
            end += 1
            if len(text) != end and text[end].isalnum():
                continue
            start = end - len(key) - 1
            if start >= 0 and text[start].isalnum():
                continue
            annotations.append(Annotation(key, labels, start + 1, end))
        annotations.sort(key=self.EntityKey)
        annotations = TaxonTagger.remove_overlap(annotations)
        annotations = TaxonTagger.disambiguate(annotations)
        return self.retokenize(doc, annotations)

    def disambiguate(annotations):
        uniques = set()
        for i, anno in enumerate(annotations):
            if len(anno.label) == 1:
                uniques.update(anno.label)
        for i, anno in enumerate(annotations):
            if len(anno.label) == 1:
                continue
            candidates = uniques.intersection(anno.label)
            if candidates:
                annotations[i] = Annotation(
                    anno.name, tuple(candidates), anno.start, anno.end
                )
        return annotations

    def retokenize(self, doc, annotations):
        start = [token.idx for token in doc]
        end = [token.idx + len(token) for token in doc]
        spans = []
        for annotation in annotations:
            if annotation.start in start and annotation.end in end:
                s = start.index(annotation.start)
                e = end.index(annotation.end)
                span = Span(doc, s, e + 1, label="TAXON")
                span._.set("id_candidates", annotation.label)
                spans.append(span)
        if spans:
            if doc.ents:
                tree = IntervalTree()
                for ent in doc.ents:
                    tree[ent.start:ent.end] = ent
                for span in spans:
                    tree.remove_overlap(span.start, span.end)
                    tree.addi(span.start, span.end, span)
                spans = tuple(span for (_, _, span,) in tree)
                doc.ents = spans
            else:
                try:
                    doc.ents += tuple(spans)
                except ValueError as e:
                    if e.args[0].startswith('[E103]'):
                        # Trying to set conflicting doc.ents
                        # Should have been resolved by remove_overlap (ents from same tagger)
                        # and the use of an interval tree (ents from different tagger)
                        span_re = re.compile(f"'\\((\\d+),\\s(\\d+),\\s'{TaxonTagger.label}'\\)'")
                        message = e.args[0]
                        m = span_re.search(message)
                        if not m is None:
                            start1 = int(m.group(1))
                            end1 = int(m.group(2))
                            m = span_re.search(message, m.end()+1)
                            if not m is None:
                                start2 = int(m.group(1))
                                end2 = int(m.group(2))
                                logging.error(e)
                                logging.error("First span: %s, second span: %s", doc[start1:end1].text, doc[start2:end2].text)
                            else:
                                logging.error(e)
                        else:
                            logging.error(e)
                    else:
                        logging.error(e)
                    return doc
            with doc.retokenize() as retok:
                for span in spans:
                    if span.end - span.start > 1:
                        retok.merge(doc[span.start : span.end])
        return doc

    def entity_sort(entity1: Annotation, entity2: Annotation) -> int:
        """
        A comparison function for Annotations.
    
        Parameters
        ----------
        entity1 : Annotation
            An Annotation.
        entity2 : Annotation
            Another Annotation.
    
        Returns
        -------
        int
            -1, if entity1 starts sooner
             1, if entity1 starts later
             the difference between the end of entity2 and entity1 otherwise.
    
        """
        if entity1.start < entity2.start:
            return -1
        if entity1.start > entity2.start:
            return 1
        return entity2.end - entity1.end

    def remove_overlap(entities: List[Annotation]) -> List[Annotation]:
        """
        Removes shortes matches.
        E.g. when 'hydrogen peroxide' and 'hydrogen' have overlapping
        annotations, 'hydrogen peroxide' is returned.
    
        Parameters
        ----------
        entities : List[Annotation]
            A list of entities extracted from a text.
    
        Returns
        -------
        List[Annotation]
            The widest annotations in case of an overlap.
    
        """
        filtered = []
        start = -1
        end = -1
        # util.filter_spans got introduced in spaCy 2.1.4 (May 12, 2019)
        # https://spacy.io/api/top-level#util.filter_spans
        # We keep this function since it is older and tested.
        for entity in entities:
            # The first entity will never satisfy these conditions
            if entity.start >= start and entity.start <= end:
                continue

            filtered.append(entity)
            start = entity.start
            end = entity.end
        return filtered
