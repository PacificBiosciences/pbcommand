"""Working doc for Condition data models

https://gist.github.com/mpkocher/347f9ae9092c24888e1c702a916276c2

"""
from collections import namedtuple


class ReseqCondition(namedtuple("ReseqCondition", "cond_id subreadset alignmentset referenceset")):
    def to_dict(self):
        return {"condId": self.cond_id,
                "subreadset": self.subreadset,
                "alignmentset": self.alignmentset,
                "referenceset": self.referenceset}

    @staticmethod
    def from_dict(d):
        def _f(k):
            # sloppy
            return d[k].encode('ascii', 'ignore')

        return ReseqCondition(_f('condId'), _f('subreadset'), _f('alignmentset'), _f('referenceset'))


class ReseqConditions(namedtuple("ReseqConditions", "conditions")):
    # leave out the pipeline id. Not sure if this is necessary
    def to_dict(self):
        return {"conditions": [c.to_dict() for c in self.conditions]}

    @staticmethod
    def from_dict(d):
        return ReseqConditions([ReseqCondition.from_dict(x) for x in d['conditions']])
