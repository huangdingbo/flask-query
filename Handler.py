class Handler(object):
    @staticmethod
    def list_get(L, i, v=None):
        if not isinstance(L, list):
            return v
        if -len(L) <= i <= len(L):
            return L[i]
        else:
            return v

    @staticmethod
    def strToUpper(value):
        return str(value).strip().upper()
