#!/usr/bin/env python

"""
Pandoc filter to exact python code blocks and write each snippet out.
"""

from pandocfilters import toJSONFilter, Str

n = 0


def caps(key, value, format, meta):
    global n
    if key == "CodeBlock":
        py_types = value[0][1][0]
        if py_types.encode("ascii") == "python":
            code_block = value[-1]
            # eval(code_block)
            with open("readme-snippet-{n}.py".format(n=n), 'a') as f:
                f.write("# example {k}-{n}\n".format(k=key, n=n))
                f.write("{v}\n".format(v=code_block))

            n += 1


if __name__ == "__main__":
    toJSONFilter(caps)
