"""The MariaDB kernel"""
# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.autocomple_manager import AutoCompletionManager
from ipykernel.kernelbase import Kernel

from ._version import version as __version__
from mariadb_kernel.client_config import ClientConfig
from mariadb_kernel.mariadb_client import (
    MariaDBClient,
    ServerIsDownError,
    ContinuationPromptError,
)
from mariadb_kernel.code_parser import CodeParser
from mariadb_kernel.mariadb_server import MariaDBServer
from mariadb_kernel.maria_magics.maria_magic import MariaMagic

import logging
import pexpect
import re
import pandas
from bs4 import BeautifulSoup


class MariaDBKernel(Kernel):
    implementation = "MariaDB"
    implementation_version = __version__
    language_info = {
        "name": "SQL",
        "mimetype": "text/plain",
        "file_extension": ".sql",
    }
    banner = "MariaDB kernel"

    def __init__(self, **kwargs):
        Kernel.__init__(self, **kwargs)
        self.log.setLevel(logging.INFO)
        self.delimiter = ";"
        self.client_config = ClientConfig(self.log)
        self.mariadb_client = MariaDBClient(self.log, self.client_config)
        self.mariadb_server = None
        self.data = {"last_select": pandas.DataFrame([])}

        try:
            self.mariadb_client.start()
            self._create_completion_manager()
        except ServerIsDownError:
            if not self.client_config.start_server():
                self.log.error(
                    "The options passed through mariadb_kernel.json "
                    "prevent the kernel from starting a testing "
                    "MariaDB server instance"
                )
                raise

            # Start a single MariaDB server for a better experience
            # if user wants to quickly test the kernel
            self.mariadb_server = MariaDBServer(self.log, self.client_config)
            self.mariadb_server.start()

            # Reconnect the client now that the server is up
            if self.mariadb_server.is_up():
                self.mariadb_client.start()
                self._create_completion_manager()

    def _create_completion_manager(self):
        self.complete_manager = AutoCompletionManager(
            None,
            self.client_config.default_config["user"],
            self.client_config.default_config["password"],
            self.client_config.default_config["host"],
            int(self.client_config.default_config["port"]),
            self.log,
        )

    def get_delimiter(self):
        return self.delimiter

    def set_delimiter(self, delimiter):
        self.mariadb_client.run_statement(f"delimiter {delimiter}")
        self.delimiter = delimiter

    def _execute_magics(self, magics):
        for magic in magics:
            magic.execute(self, self.data)

    def _update_data(self, result_html):
        if not result_html or not result_html.startswith("<TABLE"):
            return

        df = pandas.read_html(result_html)
        self.data["last_select"] = df[0]

    def _styled_result(self, result_html):
        if not result_html or not result_html.startswith("<TABLE"):
            return result_html

        soup = BeautifulSoup(result_html)
        cells = soup.find_all(["td", "th"])
        for cell in cells:
            cell["style"] = "text-align:left;white-space:pre"

        return str(soup)

    def _send_message(self, stream, message):
        error = {"name": stream, "text": message + "\n"}
        self.send_response(self.iopub_socket, "stream", error)

    def do_execute(
        self, code, silent, store_history=True, user_expressions=None, allow_stdin=False
    ):
        if code.startswith("use "):
            table_name = code.rstrip(";").lstrip("use ")
            self.complete_manager.change_db(table_name)
            self.log.info("do_execute do change table to ", table_name)

        rv = {
            "status": "ok",
            # The base class increments the execution count
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }

        try:
            parser = CodeParser(self.log, code, self.delimiter)
        except ValueError as e:
            self._send_message("stderr", str(e))
            return rv

        statements = parser.get_sql()
        for s in statements:
            try:
                result = self.mariadb_client.run_statement(s)
            except ContinuationPromptError:
                self._send_message(
                    "stderr",
                    "We detected a continuation prompt in the output coming from the MariaDB Client.\n"
                    + "Most probably you forgot to close the quotes for a string.\n"
                    + "Please fix the error in the statement and try again.",
                )
                return rv

            if self.mariadb_client.iserror():
                self._send_message("stderr", self.mariadb_client.error_message())
                continue

            self._update_data(result)
            result_str = str(result)
            if not silent:
                display_content = {
                    "data": {"text/html": self._styled_result(result_str)},
                    "metadata": {},
                }
                self.send_response(self.iopub_socket, "display_data", display_content)

        self._execute_magics(parser.get_magics())

        return rv

    def do_shutdown(self, restart):
        self.mariadb_client.stop()
        if self.mariadb_server:
            self.mariadb_server.stop()

    def do_complete(self, code, cursor_pos):
        self.log.info("code : ", code)
        completion_list = self.complete_manager.get_completions(code, cursor_pos)
        match_text_list = [completion.text for completion in completion_list]
        self.log.info("match_text_list", match_text_list)
        offset = 0
        if len(completion_list) > 0:
            offset = completion_list[
                0
            ].start_position  # if match part is 'sel', then start_position would be -3
        return {
            "status": "ok",
            "matches": match_text_list,
            "cursor_start": cursor_pos + offset,
            "cursor_end": cursor_pos,
        }
