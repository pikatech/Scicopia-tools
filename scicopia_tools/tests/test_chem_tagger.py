#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 15:23:40 2020

@author: kampe
"""

from scicopia_tools.components.ChemTagger import ChemTagger

import pytest

@pytest.fixture
def pipeline():
    import spacy

    nlp = spacy.load("en_core_web_sm")
    dict_path = "scicopia_tools/tests/resources/chemicals.txt"
    with open(dict_path, "rt") as chemicals:
        chemtagger = ChemTagger(chemicals, "chemicals")
    nlp.add_pipe(chemtagger, after='tagger')
    return nlp


def test_overlap(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Hydrogen_peroxide
    doc = pipeline(
        "Enzymes that use or decompose hydrogen peroxide are classified as peroxidases."
    )
    print(pipeline.pipeline)
    assert len(doc.ents) == 1
    assert doc.ents[0].text == "hydrogen peroxide"

def test_compounds(pipeline):
    # Taken from: https://en.wikipedia.org/wiki/Ethyl_acetate
    doc = pipeline("Ethyl acetate is the ester of ethanol and acetic acid.")
    assert len(doc.ents) == 3

def test_pos_tags(pipeline):
    doc = pipeline("Our steps lead us deeper into the lead mine.")
    assert len(doc.ents) == 1
