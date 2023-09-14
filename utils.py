import re

class Utils:
    @staticmethod
    def parameterize_name(name):
        return re.sub(r"[ \-\/\.]+", "_", name)
