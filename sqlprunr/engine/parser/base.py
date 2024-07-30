from dataclasses import dataclass
from datetime import datetime
import typing
import logging

from sql_metadata import Parser

from sqlprunr.data.generic import Column, Database, Schema, Table

from abc import ABC, abstractmethod

from sqlprunr.data.query_data import Frequencies, QueryData
from sqlprunr.engine.analyzer import analyze_query


class AbstractTableParser(ABC):
    @abstractmethod
    def parse_table(self, query: typing.Any) -> typing.List[Table]:
        """
        Parse abstract query and return a list of tables.

        :param query: Abstract query to parse
        :return: List of tables
        """
        raise NotImplementedError