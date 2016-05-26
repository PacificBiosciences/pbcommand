import json

from pbcommand.models import ReseqConditions


def load_reseq_conditions_from(json_file_or_dict):
    """
    Load resequencing conditions from JSON file or str

    :param json_file_or_dict:

    :rtype: ReseqConditions
    """

    # refactor that common useage from TC io
    if isinstance(json_file_or_dict, dict):
        d = json_file_or_dict
    else:
        with open(json_file_or_dict, 'r') as f:
            d = json.loads(f.read())

    return ReseqConditions.from_dict(d)
