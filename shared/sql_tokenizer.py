"""Fail-closed SQL tokenization shared by generation and Terraform planning."""


class SqlTokenizeError(ValueError):
    """SQL that cannot be tokenized unambiguously (for example, an open literal)."""


def sql_tokens(sql: str) -> list[str]:
    """Tokenize SQL without discarding any non-comment, non-whitespace character.

    Literals and ``$$`` bodies remain byte-exact. Unquoted words are folded to
    lower case because Databricks SQL identifiers and keywords are insensitive.
    """
    tokens: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            i = n if end < 0 else end + 1
        elif sql.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if sql.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif sql.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            if depth:
                raise SqlTokenizeError("unterminated /* comment")
            i = j
        elif sql.startswith("$$", i):
            end = sql.find("$$", i + 2)
            if end < 0:
                raise SqlTokenizeError("unterminated $$ body")
            tokens.append(sql[i:end + 2])
            i = end + 2
        elif ch in "'\"`":
            backslash = ch != "`" and not (i > 0 and sql[i - 1] in "rR" and tokens[-1:] == ["r"])
            j = i + 1
            while True:
                if j >= n:
                    raise SqlTokenizeError(f"unterminated {ch} literal")
                if backslash and sql[j] == "\\":
                    j += 2
                elif sql[j] == ch and sql.startswith(ch * 2, j):
                    j += 2
                elif sql[j] == ch:
                    break
                else:
                    j += 1
            tokens.append(sql[i:j + 1])
            i = j + 1
        elif ch.isalnum() or ch == "_":
            j = i
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            tokens.append(sql[i:j].lower())
            i = j
        else:
            # Fail toward replacement: every otherwise-unclassified character
            # is itself a token and therefore contributes to the hash.
            tokens.append(ch)
            i += 1
    return tokens
