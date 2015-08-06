import unittest

from pbcommand.models import TaskTypes, FileTypes, get_pbparser


class TestParsers(unittest.TestCase):
    def test_input_output_files(self):
        p = get_pbparser(
            "pbcommand.tasks.test_parsers",
            "0.1",
            "Display Name",
            "Tool Description ",
            "pbcommand-driver-cmd",
            is_distributed=False,
            nproc=1,
            resource_types=())

        p.add_input_file_type(
            file_type=FileTypes.FASTA,
            file_id="fasta",
            name="Fasta file",
            description="Fasta file description")

        p.add_input_file_type(FileTypes.JSON,
            "json",
            "JSON file",
            "JSON file description")

        p.add_output_file_type(
            file_type=FileTypes.GFF,
            file_id="gff",
            name="GFF file",
            description="GFF file description",
            default_name="annotations.gff")
        tc_contract = p.to_contract()
        d = tc_contract.to_dict()
        inputs = d['tool_contract']['input_types']
        self.assertEqual(inputs, [
            {
                'description': 'Fasta file description',
                'title': 'Fasta file',
                'id': 'fasta',
                'file_type_id': 'PacBio.FileTypes.Fasta'
            },
            {
                'description': 'JSON file description',
                'title': 'JSON file',
                'id': 'json',
                'file_type_id': 'PacBio.FileTypes.json'
            }
        ])
        outputs = d['tool_contract']['output_types']
        self.assertEqual(outputs, [
            {
                'title': 'GFF file',
                'description': 'GFF file description',
                'default_name': 'annotations.gff',
                'id': 'gff',
                'file_type_id': 'PacBio.FileTypes.gff'
            }
        ])

    def test_misc_parser_types(self):
        p = get_pbparser(
            "pbcommand.tasks.test_parsers",
            "0.1.0",
            "Tool Name",
            "Tool Descripion",
            "pbcommand-driver-exe ")

        p.add_int("pbcommand.task_options.n", "n", default=0, name="N",
            description="Integer option")

        p.add_float("pbcommand.task_options.f", "f", default=0, name="F",
            description="Float option")

        # XXX note that the 'default' value is not actually what the option is
        # set to by default - it simply signals that action=store_true
        p.add_boolean("pbcommand.task_options.loud", "loud", default=False,
                      name="Verbose", description="Boolean option")

        pa = p.arg_parser.parser.parse_args

        opts = pa(["--n", "250", "--f", "1.2345", "--loud"])
        self.assertEqual(opts.n, 250)
        self.assertEqual(opts.f, 1.2345)
        self.assertTrue(opts.loud)

        opts2 = pa([])
        self.assertFalse(opts2.loud)

    # TODO we should add a lot more tests for parser behavior

if __name__ == "__main__":
    unittest.main()
