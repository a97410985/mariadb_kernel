from typing import List
from mycli.sqlcompleter import SQLCompleter
from re import compile
from prompt_toolkit.completion import Completion


class DummySqlCompleter(SQLCompleter):
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

    def get_completions(
        self, document, complete_event, smart_completion=None
    ) -> List[Completion]:
        print("my completions")
        return [
            Completion("test", display_meta="test"),
            Completion("test2", display_meta="test2"),
        ]
