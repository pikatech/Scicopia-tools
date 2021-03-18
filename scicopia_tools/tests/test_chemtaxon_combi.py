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

    nlp = spacy.load("en_core_web_sm", disable=["ner"])
    dict_path = "scicopia_tools/tests/resources/taxa.tsv"
    with open(dict_path, "rt") as taxa:
        taxontagger = TaxonTagger(taxa)
    # tagger -> taxontagger
    nlp.add_pipe(taxontagger, after="tagger")
    chemicals_path = "scicopia_tools/tests/resources/chemicals.txt"
    with open(chemicals_path, "rt") as chemicals:
        chemtagger = ChemTagger(chemicals, "CHEMICAL")
    # tagger -> chemtagger -> taxontagger
    nlp.add_pipe(chemtagger, after="tagger")
    return nlp


def test_recognition_overlap(pipeline):
    doc = pipeline("A water buffalo is more common than a nitrogen buffalo.")
    assert len(doc.ents) == 2
    assert [ent.text for ent in doc.ents] == ["water buffalo", "nitrogen"]
