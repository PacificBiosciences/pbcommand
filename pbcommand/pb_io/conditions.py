import json
import os

from pbcommand.models import ReseqConditions, ReseqCondition


def _resolve_conditions(cs, path):
    """
    :type cs: ReseqConditions
    :rtype: ReseqConditions
    """

    def _resolve_if(p):
        if os.path.isabs(p):
            return p
        else:
            return os.path.join(path, p)

    rconditions = []
    for c in cs.conditions:
        s = _resolve_if(c.subreadset)
        a = _resolve_if(c.alignmentset)
        r = _resolve_if(c.referenceset)
        rc = ReseqCondition(c.cond_id, s, a, r)
        rconditions.append(rc)

    return cs._replace(conditions=rconditions)


def load_reseq_conditions_from(json_file_or_dict):
    """
    Load resequencing conditions from JSON file or str

    :param json_file_or_dict:

    :rtype: ReseqConditions
    """

    # refactor that common usage from TC io
    if isinstance(json_file_or_dict, dict):
        d = json_file_or_dict
    else:
        with open(json_file_or_dict, 'r') as f:
            d = json.loads(f.read())

    cs = ReseqConditions.from_dict(d)

    # Resolve
    if isinstance(json_file_or_dict, str):
        dir_name = os.path.dirname(os.path.abspath(json_file_or_dict))
        return _resolve_conditions(cs, dir_name)
    else:
        return cs
