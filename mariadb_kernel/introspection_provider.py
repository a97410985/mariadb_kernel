from typing import Dict, Union
from mariadb_kernel.autocompleter import Autocompleter
from mariadb_kernel.sql_analyze import SQLAnalyze
import re
from prompt_toolkit.document import Document
from mariadb_kernel.completion_engine import suggest_type, last_word
from mock import Mock


def first_word(text):
    if not text:  # Empty string
        return ""
    else:
        regex = re.compile(r"^(\w+)")
        matches = regex.search(text)
        if matches:
            return matches.group(0)
        else:
            return ""


class IntrospectionProvider:
    def get_instropection(self, document: Document, completer: SQLAnalyze):
        # return word's type and word's text
        last_partial_token_right = first_word(document.text_after_cursor)
        last_partial_token_left = last_word(document.text_before_cursor)
        word = last_partial_token_left + last_partial_token_right

        # get word start's position and end's position
        start_position = document.cursor_position - len(last_partial_token_left)
        end_position = document.cursor_position + len(last_partial_token_right)
        suggestion = suggest_type(document.text, document.text[:start_position])
        suggest_dict = {}
        for suggest in suggestion:
            type = suggest.get("type")
            if type:
                suggest_dict[type] = suggest
        # Need to check it is being suggested and it exists or not
        # priority: keyword -> function -> databse -> table -> column
        if suggest_dict.get("keyword") and word in [
            keyword.lower() for keyword in completer.keywords
        ]:
            return {"word": word, "type": "keyword"}
        elif suggest_dict.get("function") and word in [
            function.lower() for function in completer.functions
        ]:
            return {"word": word, "type": "function"}
        elif suggest_dict.get("database") and word in completer.databases:
            return {"word": word, "type": "database"}
        elif suggest_dict.get("table"):
            # If suggest_dict's table has schema field. Regard that as the database, which table belongs
            if suggest_dict["table"].get("schema"):
                table_dbName = suggest_dict["table"].get("schema")
                for t in completer.database_tables:
                    if t[0] == table_dbName and t[1] == word:
                        return {"word": word, "type": "table", "database": table_dbName}
            else:
                table_dbName = completer.dbname
                curDB = completer.dbmetadata["tables"].get(table_dbName)
                if curDB:
                    if curDB.get(word):
                        return {"word": word, "type": "table", "database": table_dbName}
        elif suggest_dict.get("column"):
            # if suggest_dict's column has tables not empty, use that as table
            tables = suggest_dict["column"].get("tables")
            curDB: Union[Dict, None] = completer.dbmetadata["tables"].get(
                completer.dbname
            )
            if len(tables) > 0:
                if tables[0][1] != None and curDB:
                    if curDB.get(tables[0][1]) and word in curDB[tables[0][1]]:
                        return {
                            "word": word,
                            "type": "column",
                            "database": completer.dbname,
                            "table": tables[0][1],
                        }
            # search all table for finding which table contains this column
            if curDB:
                for key in curDB.keys():
                    if word in curDB[key]:
                        # maybe this is wrong
                        return {
                            "word": word,
                            "type": "column",
                            "database": completer.dbname,
                            "table": key,
                        }

    def get_introspection_explain_html(
        self, document: Document, autocompleter: Autocompleter
    ):
        result = self.get_instropection(document, autocompleter.completer)
        if result:
            word_type = result.get("type")
            if word_type == "keyword":
                return "<b>keyword</b>"
            elif word_type == "function":
                return "<b>function</b>"
            elif word_type == "database":
                word = result.get("word")
                if word:
                    tables_html = autocompleter.executor.get_tables_in_db_html(word)
                    return f"<b>database</b><br/>{tables_html}"
                else:
                    return "<b>database</b>"
            elif word_type == "table":
                word = result.get("word")
                db_name = result.get("database")
                if word and db_name:
                    table_html = autocompleter.executor.get_table_schema_html(
                        word, db_name
                    )
                    limit_num = 5
                    table_rows_html = autocompleter.executor.get_partial_table_row_html(word, db_name, limit_num)
                    return f"<b>table</b><br/>{table_html}<b>first {limit_num} row of the table {word}</b><br/>{table_rows_html}"
                else:
                    return "<b>table</b>"
            elif word_type == "column":
                word = result.get("word")
                table_name = result.get("table")
                db_name = result.get("database")
                if word and db_name and table_name:
                    column_html = autocompleter.executor.get_column_type_html(
                        word, table_name, db_name
                    )
                    limit_num = 5
                    column_rows_html = autocompleter.executor.get_column_row_html(word, table_name, db_name, limit_num)
                    return f"<b>column</b><br/>{column_html}<br/><b>first {limit_num} row of the column {word}</b><br/>{column_rows_html}"
                else:
                    return "<b>column</b>"


# example
# introspection_provider = IntrospectionProvider()
# completer = SQLAnalyze(Mock())
# result = introspection_provider.get_instropection(
#     Document("select col1 from tbl1", len("select col")), completer
# )
# print(result)
# introspection_provider.get_instropection(
#     Document("select col1 from ", len("select col")), completer
# )
# introspection_provider.get_instropection(
#     Document("insert into db1.tbl1 ", len("insert into db1.t")), completer
# )
# introspection_provider.get_instropection(
#     Document("insert into db2.tbl1 ", len("insert into db2.t")), completer
# )
