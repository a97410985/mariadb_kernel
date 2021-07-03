from mycli.sqlcompleter import SQLCompleter


class SQLAnalyze(SQLCompleter):
    def __init__(
        self, smart_completion=True, supported_formats=(), keyword_casing="auto"
    ):
        super().__init__(
            smart_completion=smart_completion,
            supported_formats=supported_formats,
            keyword_casing=keyword_casing,
        )
