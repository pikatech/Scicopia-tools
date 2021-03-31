#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 20:22:32 2021

@author: tech
"""
import logging
from collections import namedtuple
from typing import NamedTuple

from pyArango.collection import Collection
from pyArango.connection import Connection
from pyArango.database import Database

from scicopia_tools.config import read_config
from scicopia_tools.exceptions import ConfigError, DBError

DbAccess = namedtuple("DbAccess", ["collection", "connection", "database"])


def setup() -> DbAccess:
    """
    Connect to the Arango database.

    Returns
    -------
    NamedTuple[Collection, Connection, Database]
        1. The ArangoDB collection that holds the scientific documents
        2. An open connection to the ArangoDB instance
        3. A handle to the database that holds the collection

    Raises
    ------
    ConfigError
        If a needed entry is missing in the config file.
    DBError
        If the connection to the ArangoDB server failed or the database or the collection can not be found.
    """
    config = read_config()
    if not "username" in config:
        raise ConfigError("Setting missing in config file: 'username'.")
    if not "password" in config:
        raise ConfigError("Setting missing in config file: 'password'.")
    try:
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
    except:
        raise DBError(f"Connection to the ArangoDB server failed.")

    if not "database" in config:
        raise ConfigError("Setting missing in config file: 'database'.")
    if connection.hasDatabase(config["database"]):
        db = connection[config["database"]]
    else:
        raise DBError(f"Database '{config['database']}' not found.")

    if not "documentcollection" in config:
        raise ConfigError("Setting missing in config file: 'documentcollection'.")
    if db.hasCollection(config["documentcollection"]):
        collection = db[config["documentcollection"]]
    else:
        raise DBError(f"Collection '{config['documentcollection']}' not found.")
    return DbAccess(collection, connection, db)
