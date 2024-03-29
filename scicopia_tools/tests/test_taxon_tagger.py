#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 15:08:07 2020

@author: tech
"""

import pytest

from scicopia_tools.components.TaxonTagger import TaxonTagger


@pytest.fixture
def pipeline():
    import spacy

    nlp = spacy.load("en_core_web_sm", exclude=["ner", "lemmatizer", "textcat"])
    dict_path = "scicopia_tools/tests/resources/taxa.tsv"
    nlp.add_pipe("taxontagger", config={"wordlist": dict_path}, after="tagger")
    return nlp


def test_overlap(pipeline):
    doc = pipeline("The Burkholderia pseudomallei group contains overlapping mentions.")
    assert len(doc.ents) == 1
    assert doc.ents[0].text == "Burkholderia pseudomallei"


def test_ambiguous(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Caenorhabditis_elegans
    doc = pipeline(
        "C. elegans is an unsegmented pseudocoelomate and lacks respiratory or circulatory systems."
    )
    assert len(doc.ents) == 1
    entity = doc.ents[0]
    assert len(entity._.id_candidates) == 3
    assert list(entity._.id_candidates) == ["3937", "4853", "6239"]


def test_disambiguation(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Caenorhabditis_elegans
    doc = pipeline(
        "Caenorhabditis elegans is a free-living transparent nematode about 1 mm in length that lives in temperate soil environments. C. elegans is an unsegmented pseudocoelomate and lacks respiratory or circulatory systems."
    )
    assert len(doc.ents) == 2
    assert [ent.text for ent in doc.ents] == ["Caenorhabditis elegans", "C. elegans"]
    assert list(doc.ents[1]._.id_candidates) == ["6239"]


def test_negative(pipeline):
    doc = pipeline("Patricia likes Caenorhabditis elegans.")
    assert len(doc.ents) == 1
    assert [ent.text for ent in doc.ents] == ["Caenorhabditis elegans"]
