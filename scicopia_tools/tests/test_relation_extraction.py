#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 22 15:08:07 2020

@author: tech
"""

import pytest

from scicopia_tools.components.ChemTagger import ChemTagger
from scicopia_tools.components.TaxonTagger import TaxonTagger


@pytest.fixture
def pipeline():
    import spacy

    nlp = spacy.load("en_core_web_sm", disable=["ner"])
    taxa_path = "scicopia_tools/tests/resources/taxa.tsv"
    with open(taxa_path, "rt") as taxa:
        taxontagger = TaxonTagger(taxa)
    nlp.add_pipe(taxontagger, after="tagger")
    chemicals_path = "scicopia_tools/tests/resources/chemicals.txt"
    with open(chemicals_path, "rt") as chemicals:
        chemtagger = ChemTagger(chemicals, "chemicals")
    nlp.add_pipe(chemtagger, after='tagger')
    return nlp


def test_recognition(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Nitrospira_moscoviensis
    doc = pipeline(
        "In aerobic environments, N. moscoviensis obtains energy by oxidizing nitrite to nitrate."
    )
    assert len(doc.ents) == 3

