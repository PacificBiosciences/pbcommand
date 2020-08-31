"""
Models carried over from pbsmrtpipe, still required for SMRT Link interface.
"""

from collections import namedtuple
import json


class Pipeline:

    def __init__(self, idx, display_name, version, description, bindings,
                 entry_points, parent_pipeline_ids=None, tags=(),
                 task_options=None):

        self.idx = idx
        self.version = version
        self.display_name = display_name
        self.description = description.strip()
        # set of [(a, b), ...]
        self.bindings = {x for x in bindings}
        # set of [(a, b), ...]
        self.entry_points = entry_points
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
    def required_inputs(self):
        return [o for o in self.entry_points if not o.optional]

    @property
    def optional_inputs(self):
        return [o for o in self.entry_points if o.optional]

    def __repr__(self):
        # Only communicate the entry id
        ek = [e["entryId"] for e in self.entry_points]
        e = " ".join(ek)
        _d = dict(k=self.__class__.__name__, i=self.idx,
                  d=self.display_name, b=len(self.bindings), e=e)
        return "<{k} id={i} nbindings={b} entry bindings={e} >".format(**_d)

    def summary(self, verbose=True):
        outs = []
        f = outs.append

        def _format_inputs(inputs):
            return ", ".join([ep.short_name for ep in inputs])

        f("Workflow Summary")
        f("Workflow Id    : {}".format(self.pipeline_id))
        f("Name           : {}".format(self.display_name))
        f("Description    : {}".format(self.description))
        f("Required Inputs: {}".format(_format_inputs(self.required_inputs)))
        if len(self.optional_inputs) > 0:
            f("Optional Inputs: {}".format(_format_inputs(self.optional_inputs)))
        if self.tags:
            f("Tags           : {} ".format(", ".join(list(set(self.tags)))))
        if len(self.task_options) > 0:
            f("Task Options:")
            for opt in self.task_options:
                f("  {o} = {v}".format(o=opt.option_id, v=opt.default))
                if verbose:
                    f("    {n} ({t})".format(n=opt.name, t=opt.OPTION_TYPE_ID))
        return "\n".join(outs)
