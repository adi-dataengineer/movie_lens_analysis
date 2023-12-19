class CustomErrors(Exception):
    """Class to handle Custom Exceptions that would be raised for errors"""

    class MultiFileError(Exception):
        """Exception to be raised when same file exists more than once in zip archive"""

    class DpDqtError(Exception):
        """Exception to be raised when same file exists more than once in zip archive"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)
