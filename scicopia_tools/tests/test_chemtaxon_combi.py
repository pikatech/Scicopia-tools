#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 26 14:57:45 2020

@author: tech
"""

import pytest

from scicopia_tools.components.ChemTagger import ChemTagger
from scicopia_tools.components.TaxonTagger import TaxonTagger


@pytest.fixture
def pipeline():
    import spacy

    nlp = spacy.load("en_core_web_sm", exclude=["ner", "lemmatizer", "textcat"])
    dict_path = "scicopia_tools/tests/resources/taxa.tsv"
    nlp.add_pipe("taxontagger", config={"wordlist": dict_path}, after="tagger")
    chemicals_path = "scicopia_tools/tests/resources/chemicals.txt"
    # tagger -> chemtagger -> taxontagger
    nlp.add_pipe("chemtagger", config={"wordlist": chemicals_path}, after="tagger")
    return nlp


def test_recognition_overlap(pipeline):
    doc = pipeline("A water buffalo is more common than a nitrogen buffalo.")
    assert len(doc.ents) == 2
    assert [ent.text for ent in doc.ents] == ["water buffalo", "nitrogen"]
