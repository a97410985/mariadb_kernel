import re
from mariadb_kernel.autocomple_manager import AutoCompletionManager
from prompt_toolkit.document import Document
from time import sleep
from unittest.mock import Mock

autoCompletionManager = AutoCompletionManager(None, "root", "", "localhost", 3306, None)
sleep(1)
result = autoCompletionManager.get_completions(text="sel", cursor_position=3)
print(result)
