from mycli import sqlcompleter, sqlexecute, completion_refresher
from prompt_toolkit.document import Document
from tornado.gen import sleep


class AutoCompletionManager:
    cur_completer = None
    cur_executer = None
    cur_completion_refresher = None
    log = None
    # port must be int
    def __init__(self, database, user, password, host, port, log):
        self.cur_executer = sqlexecute.SQLExecute(
            database,
            user,
            password,
            host,
            port,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        self.cur_completion_refresher = completion_refresher.CompletionRefresher()
        self.cur_completion_refresher.refresh(
            self.cur_executer,
            self._on_completions_refreshed,
            {"smart_completion": True},
        )
        self.log = log

    def change_db(self, dbName):
        self.cur_executer.change_db(dbName)
        self.cur_completer.set_dbname(dbName)
        self.log.info("change_db")
        self.cur_completion_refresher.refresh(
            self.cur_executer,
            self._on_completions_refreshed,
            {"smart_completion": True},
        )

    def _on_completions_refreshed(self, new_completer):
        """Swap the completer object in cli with the newly created completer."""
        self.cur_completer = new_completer
        if self.log is not None:
            self.log.info("_on_completions_refreshed")
            self.log.info("dbmetadata : ", self.cur_completer.dbmetadata)

    def get_completions(self, text, cursor_position):
        return self.cur_completer.get_completions(
            Document(text=text, cursor_position=cursor_position), None
        )
