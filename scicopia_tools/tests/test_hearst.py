#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 15:23:40 2020

@author: kampe
"""

from scicopia_tools.analyzers.Hearst import Hearst
from scicopia_tools.components.ChemTagger import ChemTagger


def test_such_as():
    chemicals_path = "scicopia_tools/tests/resources/chemicals.txt"
    extra = [{"component": "chemtagger", "config": {"wordlist": chemicals_path}}]
    hearst = Hearst("en_core_web_sm", extra)
    # Taken from: https://en.wikipedia.org/wiki/Ethyl_acetate
    text = "Ascorbic acid is a redox catalyst which can reduce, and thereby neutralize, reactive oxygen species such as hydrogen peroxide."
    relations = hearst.process(text)[Hearst.field]
    assert len(relations) == 1
    assert " ".join(relations[0]) == "reactive oxygen species such as hydrogen peroxide"
