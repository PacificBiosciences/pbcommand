"""
Models carried over from pbsmrtpipe, still required for SMRT Link interface.
"""

import itertools
import json


class Pipeline(object):

    def __init__(self, idx, display_name, version, description, bindings, entry_bindings, parent_pipeline_ids=None, tags=(), task_options=None):
        """

        Both entry_points and bindings are provided as "simple" format (e.g, [("alpha:0", "beta:1"])

        This really should have been abstracted away into containers to make the interface clear. This was a fundamental
        design mistake.

        :param bindings: List of "simple" binding format [("alpha:0:0", "beta:0:0")]
        :param entry_bindings: List of "simple" bindings [("$entry:e1", "my_task:0")]
        """

        self.idx = idx
        self.version = version
        self.display_name = display_name
        self.description = description.strip()
        # set of [(a, b), ...]
        self.bindings = {x for x in bindings}
        # set of [(a, b), ...]
        self.entry_bindings = {x for x in entry_bindings}
        # list of strings
        self.tags = tags
        if parent_pipeline_ids is None:
            self.parent_pipeline_ids = []
        else:
            self.parent_pipeline_ids = parent_pipeline_ids
        # Task Level options
        self.task_options = {} if task_options is None else task_options

    @property
    def pipeline_id(self):
        return self.idx

    @property
    def all_bindings(self):
        return self.bindings | self.entry_bindings

    def __repr__(self):
        # Only communicate the entry id
        ek = [eid for eid, _ in self.entry_bindings]
        e = " ".join(ek)
        _d = dict(k=self.__class__.__name__, i=self.idx,
                  d=self.display_name, b=len(self.bindings), e=e)
        return "<{k} id={i} nbindings={b} entry bindings={e} >".format(**_d)

    def summary(self):
        outs = []
        f = outs.append

        # out a list of tuples
        def _printer(xs):
            for a, b in xs:
                sx = ' -> '.join([str(a), str(b)])
                f("  " + sx)

        f("Pipeline Summary")
        f("Pipeline Id: {}".format(self.pipeline_id))
        f("Name       : {}".format(self.display_name))
        f("Description: {}".format(self.description))
        f("EntryPoints: {}".format(len(self.entry_bindings)))
        _printer(self.entry_bindings)
        if self.tags:
            f("Tags       : {} ".format(", ".join(list(set(self.tags)))))
        if len(self.task_options) > 0:
            f("Task Options:")
            for option_id, value in self.iter_options():
                f("  {o} = {v}".format(o=option_id, v=value))
        return "\n".join(outs)

    def iter_options(self):
        option_ids = sorted(self.task_options.keys())
        for option_id in option_ids:
            yield (option_id, self.task_options[option_id])

    @staticmethod
    def from_dict(d):
        bindings = {}  # obsolete
        epoints = [(e["entryId"], e["fileTypeId"]) for e in d["entryPoints"]]
        # The pipeline instance only needs to the key-value pair
        task_options = {o["id"]: o["default"] for o in d['taskOptions']}
        return Pipeline(d['id'], d['name'], d['version'], d['description'], bindings, epoints, tags=d['tags'], task_options=task_options)

    @staticmethod
    def load_from_json(file_name):
        with open(file_name, "r") as json_in:
            return Pipeline.from_dict(json.loads(json_in.read()))
