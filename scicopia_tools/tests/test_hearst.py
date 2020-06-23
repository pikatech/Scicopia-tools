#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 15:23:40 2020

@author: kampe
"""

from analyzers.Hearst import Hearst


def test_such_as():
    hearst = Hearst("en_core_web_sm")
    # Taken from: https://en.wikipedia.org/wiki/Ethyl_acetate
    text = "In the laboratory, and usually for illustrative purposes only, ethyl esters are typically hydrolyzed in a two-step process starting with a stoichiometric amount of a strong base, such as sodium hydroxide."
    relations = hearst.process(text)[Hearst.field]
    assert len(relations) == 1
    assert " ".join(relations[0]) == "strong base such as sodium hydroxide"
