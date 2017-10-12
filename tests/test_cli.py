from click.testing import CliRunner
import gpsdio.cli.main


def test_segment_via_cli(tmpdir):
    outfile = str(tmpdir.mkdir('out').join('segmented.json'))
    result = CliRunner().invoke(gpsdio.cli.main.main_group, [
        'segment',
        '--noise-dist=0',
        '--max-speed=30',
        'tests/data/416000000.json',
        outfile
    ])
    assert result.exit_code == 0
    with gpsdio.open(outfile) as actual,\
            gpsdio.open('tests/data/segmented-416000000.json') as expected:
        for e, a in zip(expected, actual):
            assert e == a


def test_num_points_in_equals_num_points_out(tmpdir):
    outfile = str(tmpdir.mkdir('out').join('segmented.json'))
    result = CliRunner().invoke(gpsdio.cli.main.main_group, [
        'segment',
        'tests/data/416000000.json',
        outfile
    ])
    assert result.exit_code == 0
    with gpsdio.open('tests/data/416000000.json') as e:
        expected = len(list(e))
    with gpsdio.open(outfile) as a:
        actual = len(list(a))
    assert expected == actual
