from mycli import completion_refresher, sqlcompleter
from mycli.packages.filepaths import complete_path
from mycli.sqlexecute import SQLExecute
from mariadb_kernel import dummy_sql_completer, mycli_sql_completer
import threading


class MyCompletionRefresher(completion_refresher.CompletionRefresher):
    def __init__(self, sql_completer_name: str) -> None:
        self._completer_thread = None
        self._restart_refresh = threading.Event()
        self.sql_completer_name = sql_completer_name

    def _bg_refresh(self, sqlexecute, callbacks, completer_options):
        print("_bg_refresh")
        if self.sql_completer_name == "my_cli":
            completer = mycli_sql_completer.MyCliSqlCompleter(**completer_options)
        elif self.sql_completer_name == "dummy":
            completer = dummy_sql_completer.DummySqlCompleter(**completer_options)

        print("completer : ", completer)

        # Create a new pgexecute method to popoulate the completions.
        e = sqlexecute
        executor = SQLExecute(
            e.dbname,
            e.user,
            e.password,
            e.host,
            e.port,
            e.socket,
            e.charset,
            e.local_infile,
            e.ssl,
            e.ssh_user,
            e.ssh_host,
            e.ssh_port,
            e.ssh_password,
            e.ssh_key_filename,
        )

        # If callbacks is a single function then push it into a list.
        if callable(callbacks):
            callbacks = [callbacks]

        while 1:
            for refresher in self.refreshers.values():
                refresher(completer, executor)
                if self._restart_refresh.is_set():
                    self._restart_refresh.clear()
                    break
            else:
                # Break out of while loop if the for loop finishes natually
                # without hitting the break statement.
                break

            # Start over the refresh from the beginning if the for loop hit the
            # break statement.
            continue

        for callback in callbacks:
            callback(completer)
