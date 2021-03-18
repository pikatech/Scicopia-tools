#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 20:22:32 2021

@author: tech
"""
import logging
from typing import Tuple

from pyArango.collection import Collection
from pyArango.connection import Connection
from pyArango.database import Database

from scicopia_tools.config import read_config


def setup() -> Tuple[Collection, Connection, Database]:
    config = read_config()
    if "arango_url" in config:
        connection = Connection(
            arangoURL=config["arango_url"],
            username=config["username"],
            password=config["password"],
        )
    else:
        connection = Connection(
            username=config["username"], password=config["password"]
        )

    if connection.hasDatabase(config["database"]):
        db = connection[config["database"]]
    else:
        logging.error("Database %s not found.", config["database"])

    if db.hasCollection(config["collection"]):
        collection = db[config["collection"]]
    else:
        logging.error("Collection %s not found.", config["collection"])

    return collection, connection, db
