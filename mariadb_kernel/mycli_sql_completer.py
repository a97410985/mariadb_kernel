from ast import Str
from typing import Generator, Iterable, List
from mycli.sqlcompleter import SQLCompleter
from re import compile
from collections import Counter

from mycli.packages.completion_engine import suggest_type
from mycli.packages.special.favoritequeries import FavoriteQueries

import logging

from prompt_toolkit.completion.base import Completion

_logger = logging.getLogger(__name__)


class MyCliSqlCompleter(SQLCompleter):
    def extend_with_type(
        self, list: List, completions: Generator[Completion, None, None], type: Str
    ):
        new_comletion_list = []
        for completion in completions:
            new_comletion_list.append(
                Completion(
                    text=completion.text,
                    start_position=completion.start_position,
                    display_meta=type,
                )
            )
        list.extend(new_comletion_list)

    def __init__(
        self, smart_completion=True, supported_formats=(), keyword_casing="auto"
    ):
        # ! Avoid using super() method becuase would occur infinite loop. It's about self.__class__ problem (っ °Д °;)っ
        self.smart_completion = smart_completion
        self.reserved_words = set()
        for x in self.keywords:
            self.reserved_words.update(x.split())
        self.name_pattern = compile(r"^[_a-z][_a-z0-9\$]*$")

        self.special_commands = []
        self.table_formats = supported_formats
        if keyword_casing not in ("upper", "lower", "auto"):
            keyword_casing = "auto"
        self.keyword_casing = keyword_casing
        self.reset_completions()

    def get_completions(self, document, complete_event, smart_completion=None):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        if smart_completion is None:
            smart_completion = self.smart_completion

        # If smart_completion is off then match any word that starts with
        # 'word_before_cursor'.
        if not smart_completion:
            return self.find_matches(
                word_before_cursor, self.all_completions, start_only=True, fuzzy=False
            )

        completions = []
        suggestions = suggest_type(document.text, document.text_before_cursor)

        for suggestion in suggestions:

            _logger.debug("Suggestion type: %r", suggestion["type"])

            if suggestion["type"] == "column":
                tables = suggestion["tables"]
                _logger.debug("Completion column scope: %r", tables)
                scoped_cols = self.populate_scoped_cols(tables)
                if suggestion.get("drop_unique"):
                    # drop_unique is used for 'tb11 JOIN tbl2 USING (...'
                    # which should suggest only columns that appear in more than
                    # one table
                    scoped_cols = [
                        col
                        for (col, count) in Counter(scoped_cols).items()
                        if count > 1 and col != "*"
                    ]
                cols = self.find_matches(word_before_cursor, scoped_cols)
                self.extend_with_type(completions, cols, suggestion["type"])

            elif suggestion["type"] == "function":
                # suggest user-defined functions using substring matching
                funcs = self.populate_schema_objects(suggestion["schema"], "functions")
                user_funcs = self.find_matches(word_before_cursor, funcs)
                self.extend_with_type(completions, user_funcs, suggestion["type"])

                # suggest hardcoded functions using startswith matching only if
                # there is no schema qualifier. If a schema qualifier is
                # present it probably denotes a table.
                # eg: SELECT * FROM users u WHERE u.
                if not suggestion["schema"]:
                    predefined_funcs = self.find_matches(
                        word_before_cursor,
                        self.functions,
                        start_only=True,
                        fuzzy=False,
                        casing=self.keyword_casing,
                    )
                    self.extend_with_type(
                        completions, predefined_funcs, suggestion["type"]
                    )

            elif suggestion["type"] == "table":
                tables = self.populate_schema_objects(suggestion["schema"], "tables")
                tables = self.find_matches(word_before_cursor, tables)
                self.extend_with_type(completions, tables, suggestion["type"])

            elif suggestion["type"] == "view":
                views = self.populate_schema_objects(suggestion["schema"], "views")
                views = self.find_matches(word_before_cursor, views)
                self.extend_with_type(completions, views, suggestion["type"])

            elif suggestion["type"] == "alias":
                aliases = suggestion["aliases"]
                aliases = self.find_matches(word_before_cursor, aliases)
                self.extend_with_type(completions, aliases, suggestion["type"])

            elif suggestion["type"] == "database":
                dbs = self.find_matches(word_before_cursor, self.databases)
                self.extend_with_type(completions, dbs, suggestion["type"])

            elif suggestion["type"] == "keyword":
                keywords = self.find_matches(
                    word_before_cursor,
                    self.keywords,
                    start_only=True,
                    fuzzy=False,
                    casing=self.keyword_casing,
                )
                self.extend_with_type(completions, keywords, suggestion["type"])

            elif suggestion["type"] == "show":
                show_items = self.find_matches(
                    word_before_cursor,
                    self.show_items,
                    start_only=False,
                    fuzzy=True,
                    casing=self.keyword_casing,
                )
                self.extend_with_type(completions, show_items, suggestion["type"])

            elif suggestion["type"] == "change":
                change_items = self.find_matches(
                    word_before_cursor, self.change_items, start_only=False, fuzzy=True
                )
                self.extend_with_type(completions, change_items, suggestion["type"])

            elif suggestion["type"] == "user":
                users = self.find_matches(
                    word_before_cursor, self.users, start_only=False, fuzzy=True
                )
                self.extend_with_type(completions, users, suggestion["type"])

            elif suggestion["type"] == "special":
                special = self.find_matches(
                    word_before_cursor,
                    self.special_commands,
                    start_only=True,
                    fuzzy=False,
                )
                self.extend_with_type(completions, special, suggestion["type"])
            elif suggestion["type"] == "favoritequery":
                queries = self.find_matches(
                    word_before_cursor,
                    FavoriteQueries.instance.list(),
                    start_only=False,
                    fuzzy=True,
                )
                self.extend_with_type(completions, queries, suggestion["type"])
            elif suggestion["type"] == "table_format":
                formats = self.find_matches(
                    word_before_cursor, self.table_formats, start_only=True, fuzzy=False
                )
                self.extend_with_type(completions, formats, suggestion["type"])
            elif suggestion["type"] == "file_name":
                file_names = self.find_files(word_before_cursor)
                self.extend_with_type(completions, file_names, suggestion["type"])

        return completions
