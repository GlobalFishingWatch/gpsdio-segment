from click.testing import CliRunner
import gpsdio.cli.main


def test_segment_via_cli(tmpdir):
    outfile = str(tmpdir.mkdir('out').join('segmented.json'))
    result = CliRunner().invoke(gpsdio.cli.main.main_group, [
        'segment',
        'tests/data/416000000.json',
        outfile
    ])
    assert result.exit_code == 0
    with gpsdio.open(outfile) as actual,\
            gpsdio.open('tests/data/segmented-416000000.json') as expected:
        for e, a in zip(expected, actual):
            assert e == a
