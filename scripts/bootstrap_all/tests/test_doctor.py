import json
import io
import shutil
import os
import subprocess
import sys
import tempfile
import unittest
import hashlib
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from bootstrap_all.adapters import PreparationContext, prepare_source
from bootstrap_all.doctor import diagnose


class DoctorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        (self.root / 'config').mkdir()
        (self.root / 'etl/src/bracc_etl').mkdir(parents=True)
        (self.root / 'data/example').mkdir(parents=True)
        (self.root / 'data/example/input.csv').touch()
        (self.root / 'config/source_aliases.json').write_text(json.dumps({'sources': [{'source_id': 'example', 'runner_name': 'runner'}]}))
        runner = self.root / 'etl/src/bracc_etl/runner.py'
        runner.write_text('''class Pipeline: pass
PIPELINES: dict = {"runner": Pipeline}
@click.group()
def cli(): pass
@cli.command()
@click.option("--source")
@click.option("--neo4j-uri")
@click.option("--neo4j-user")
@click.option("--neo4j-password")
@click.option("--neo4j-database")
@click.option("--data-dir")
@click.option("--linking-tier")
def run(): pass
''')
        (self.root / 'etl/pyproject.toml').write_text('[project.scripts]\nbracc-etl = "bracc_etl.runner:cli"\n')
        self.contract = {'expected_implemented_count': 1, 'sources': [{'pipeline_id': 'example', 'acquisition_mode': 'file_manifest', 'required_inputs': ['data/example/input.csv']}]}

    def test_valid_and_missing_input(self):
        self.assertEqual(diagnose(self.root, self.contract, {'example'}), [])
        (self.root / 'data/example/input.csv').unlink()
        self.assertTrue(any('missing input' in e for e in diagnose(self.root, self.contract, {'example'})))

    def test_directory_does_not_satisfy_required_file_input(self):
        (self.root / 'data/example/input.csv').unlink()
        (self.root / 'data/example/input.csv').mkdir()
        self.assertTrue(any('missing input' in e for e in diagnose(self.root, self.contract, {'example'})))

    def test_manifest_directory_does_not_satisfy_required_input(self):
        input_path = self.root / 'data/example/input.csv'
        input_path.unlink()
        input_path.mkdir()
        result = prepare_source(self.contract['sources'][0], PreparationContext(str(self.root), lambda _: self.fail('manifest input must not run shell')))
        self.assertEqual(result.status, 'blocked_external')
        self.assertEqual(result.artifacts, [])

    def test_doctor_and_preparer_agree_on_directory_only_input(self):
        input_path = self.root / 'data/example/input.csv'
        input_path.unlink()
        input_path.mkdir()
        self.assertTrue(any('missing input' in e for e in diagnose(self.root, self.contract, {'example'})))
        result = prepare_source(self.contract['sources'][0], PreparationContext(str(self.root), lambda _: self.fail('manifest input must not run shell')))
        self.assertEqual(result.status, 'blocked_external')
        self.assertEqual(result.artifacts, [])

    def test_manifest_glob_returns_only_files_and_requires_file_per_pattern(self):
        source = self.contract['sources'][0]
        source['required_inputs'] = ['data/example/*.csv']
        directory = self.root / 'data/example/directory.csv'
        directory.mkdir()
        first_file = self.root / 'data/example/first.csv'
        first_file.touch()
        (self.root / 'data/example/input.csv').unlink()
        result = prepare_source(source, PreparationContext(str(self.root), lambda _: self.fail('manifest input must not run shell')))
        self.assertEqual(result.status, 'ready')
        self.assertEqual(result.artifacts, [str(first_file.resolve())])
        source['required_inputs'] = ['data/example/*.csv', 'data/example/*.json']
        result = prepare_source(source, PreparationContext(str(self.root), lambda _: self.fail('manifest input must not run shell')))
        self.assertEqual(result.status, 'blocked_external')
        self.assertEqual(result.artifacts, [])

    def test_download_with_only_directory_after_callback_fails(self):
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'script_download'
        source['download_commands'] = ['fetch inputs']
        (self.root / 'data/example/input.csv').unlink()

        def create_directory(command):
            (self.root / 'data/example/input.csv').mkdir()
            return subprocess.CompletedProcess(command, 0, '', '')

        result = prepare_source(source, PreparationContext(str(self.root), create_directory))
        self.assertEqual(result.status, 'failed_download')
        self.assertEqual(result.artifacts, [])

    def test_manifest_glob_with_only_matching_directory_is_blocked(self):
        source = self.contract['sources'][0]
        source['required_inputs'] = ['data/example/*.csv']
        (self.root / 'data/example/input.csv').unlink()
        (self.root / 'data/example/directory.csv').mkdir()
        result = prepare_source(source, PreparationContext(str(self.root), lambda _: self.fail('manifest input must not run shell')))
        self.assertEqual(result.status, 'blocked_external')
        self.assertEqual(result.artifacts, [])

    def test_manifest_glob_existing_directory_does_not_satisfy_each_pattern(self):
        source = self.contract['sources'][0]
        source['required_inputs'] = ['data/example/*.csv', 'data/example/*.json']
        (self.root / 'data/example/input.csv').unlink()
        (self.root / 'data/example/directory.csv').mkdir()
        result = prepare_source(source, PreparationContext(str(self.root), lambda _: self.fail('manifest input must not run shell')))
        self.assertEqual(result.status, 'blocked_external')
        self.assertEqual(result.artifacts, [])

    def test_download_with_matching_directory_then_regular_file_is_ready(self):
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'script_download'
        source['required_inputs'] = ['data/example/*.csv']
        source['download_commands'] = ['fetch inputs']
        (self.root / 'data/example/input.csv').unlink()
        calls = []

        def create_file(command):
            calls.append(command)
            (self.root / 'data/example/downloaded.csv').write_text('id\\n1\\n')
            return subprocess.CompletedProcess(command, 0, '', '')

        result = prepare_source(source, PreparationContext(str(self.root), create_file))
        self.assertEqual(calls, ['fetch inputs'])
        self.assertEqual(result.status, 'ready')
        self.assertEqual(result.artifacts, [str((self.root / 'data/example/downloaded.csv').resolve())])

    def test_blocked_external_source_does_not_call_shell(self):
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'blocked_external'
        source['download_commands'] = ['must not execute']
        result = prepare_source(source, PreparationContext(str(self.root), lambda command: self.fail(f'unexpected shell: {command}')))
        self.assertEqual(result.status, 'blocked_external')

    def test_detects_registry_runner_and_duplicate_divergence(self):
        self.assertTrue(any('registry mismatch' in e for e in diagnose(self.root, self.contract, {'other'})))
        (self.root / 'etl/src/bracc_etl/runner.py').write_text('PIPELINES: dict = {"other": Pipeline}\n')
        self.assertTrue(any('runner mismatch' in e for e in diagnose(self.root, self.contract, {'example'})))
        self.contract['sources'].append(self.contract['sources'][0])
        self.assertTrue(any('unique' in e for e in diagnose(self.root, self.contract, {'example'})))

    def test_detects_duplicate_runner_keys(self):
        (self.root / 'etl/src/bracc_etl/runner.py').write_text('class Pipeline: pass\nPIPELINES: dict = {"runner": Pipeline, "runner": Pipeline}\n')
        self.assertTrue(any('duplicate keys' in e for e in diagnose(self.root, self.contract, {'example'})))

    def test_rejects_missing_command(self):
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'script_download'
        source['download_commands'] = ['python3 /workspace/scripts/missing.py']
        self.assertTrue(any('unsupported or missing' in e for e in diagnose(self.root, self.contract, {'example'})))

    def test_rejects_string_download_commands_without_iterating_characters(self):
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'script_download'
        source['download_commands'] = 'python3 /workspace/scripts/missing.py'
        issues = diagnose(self.root, self.contract, {'example'})
        self.assertTrue(any('download_commands must be a list of strings' in e for e in issues))
        self.assertFalse(any('unsupported or missing download command' in e for e in issues))

    def test_rejects_download_commands_on_manifest_source(self):
        source = self.contract['sources'][0]
        source['download_commands'] = ['python3 /workspace/scripts/missing.py']
        self.assertTrue(any('require script_download mode' in e for e in diagnose(self.root, self.contract, {'example'})))

    def test_checks_physical_pipeline_modules_and_imported_classes(self):
        runner = self.root / 'etl/src/bracc_etl/runner.py'
        runner.write_text('from bracc_etl.pipelines.example import ExamplePipeline\nPIPELINES: dict = {"runner": ExamplePipeline}\n')
        issues = diagnose(self.root, self.contract, {'example'})
        self.assertTrue(any('missing pipeline module' in e for e in issues))
        module = self.root / 'etl/src/bracc_etl/pipelines/example.py'
        module.parent.mkdir(parents=True)
        module.write_text('class OtherPipeline: pass\n')
        issues = diagnose(self.root, self.contract, {'example'})
        self.assertTrue(any('missing ExamplePipeline' in e for e in issues))

    def test_checks_pipeline_values_resolve_to_defined_classes(self):
        (self.root / 'etl/src/bracc_etl/runner.py').write_text('PIPELINES: dict = {"runner": MissingPipeline}\n')
        issues = diagnose(self.root, self.contract, {'example'})
        self.assertTrue(any('PIPELINES references undefined class MissingPipeline' in e for e in issues))

    def test_requires_runner_cli_options_used_by_bootstrap(self):
        runner = self.root / 'etl/src/bracc_etl/runner.py'
        runner.write_text(runner.read_text().replace('@click.option("--data-dir")\n', ''))
        issues = diagnose(self.root, self.contract, {'example'})
        self.assertTrue(any('ETL run command is missing' in e for e in issues))

    def test_blocked_source_does_not_report_missing_input(self):
        self.contract['sources'][0]['acquisition_mode'] = 'blocked_external'
        (self.root / 'data/example/input.csv').unlink()
        self.assertEqual(diagnose(self.root, self.contract, {'example'}), [])

    def test_download_entrypoint_and_command_are_registered(self):
        runner = self.root / 'etl/src/bracc_etl/runner.py'
        runner.write_text("class Pipeline: pass\nPIPELINES: dict = {\"runner\": Pipeline}\n@click.group()\ndef cli(): pass\n@cli.command()\n@click.option('--source')\n@click.option('--neo4j-uri')\n@click.option('--neo4j-user')\n@click.option('--neo4j-password')\n@click.option('--neo4j-database')\n@click.option('--data-dir')\n@click.option('--linking-tier')\ndef run(): pass\n@cli.command()\n@click.option(' --output-dir'.strip())\n@click.option('--files')\n@click.option('--skip-existing/--no-skip-existing')\ndef download(): pass\n")
        # Use literal Click options as in the actual runner.
        runner.write_text(runner.read_text().replace("' --output-dir'.strip()", "'--output-dir'"))
        project = self.root / 'etl/pyproject.toml'
        self.contract['sources'][0]['pipeline_id'] = 'cnpj'
        (self.root / 'config/source_aliases.json').write_text(json.dumps({'sources': [{'source_id': 'cnpj', 'runner_name': 'runner'}]}))
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'script_download'
        source['download_commands'] = ['cd /workspace/etl && bracc-etl download --output-dir ../data/cnpj --files 10 --skip-existing']
        self.assertEqual(diagnose(self.root, self.contract, {'cnpj'}), [])
        runner.write_text(runner.read_text().replace('def download', 'def renamed'))
        self.assertTrue(any('unsupported or missing' in e for e in diagnose(self.root, self.contract, {'cnpj'})))
        runner.write_text(runner.read_text().replace('def renamed', 'def download'))
        project.write_text('[project.scripts]\nbracc-etl = "bracc_etl.runner:missing"\n')
        self.assertTrue(any('unsupported or missing' in e for e in diagnose(self.root, self.contract, {'cnpj'})))

    def test_cli_doctor_never_runs_commands(self):
        repo = Path(__file__).resolve().parents[3]
        (self.root / 'docs').mkdir()
        (self.root / 'docs/source_registry_br_v1.csv').write_text('pipeline_id,in_universe_v1,implementation_state\nexample,true,implemented\n')
        (self.root / 'config/bootstrap_all_contract.yml').write_text(json.dumps(self.contract))
        result = subprocess.run([sys.executable, str(repo / 'scripts/run_bootstrap_all.py'), '--repo-root', str(self.root), '--doctor', '--yes-reset'], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['config', 'data', 'docs', 'etl'])

    def test_cli_doctor_reports_invalid_command_without_side_effects(self):
        repo = Path(__file__).resolve().parents[3]
        (self.root / 'docs').mkdir()
        (self.root / 'docs/source_registry_br_v1.csv').write_text('pipeline_id,in_universe_v1,implementation_state\nexample,true,implemented\n')
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'script_download'
        source['download_commands'] = ['python3 /workspace/scripts/missing.py']
        (self.root / 'config/bootstrap_all_contract.yml').write_text(json.dumps(self.contract))
        result = subprocess.run([sys.executable, str(repo / 'scripts/run_bootstrap_all.py'), '--repo-root', str(self.root), '--doctor', '--yes-reset'], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 1)
        self.assertIn('unsupported or missing download command', result.stderr)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['config', 'data', 'docs', 'etl'])

    def test_real_main_terminal_preparation_skips_ingestion_and_ready_preparation_reaches_it(self):
        repo = Path(__file__).resolve().parents[3]
        (self.root / 'docs').mkdir()
        (self.root / 'docs/source_registry_br_v1.csv').write_text('pipeline_id,in_universe_v1,implementation_state\nexample,true,implemented\n')
        (self.root / 'config/bootstrap_all_contract.yml').write_text(json.dumps(self.contract))
        runner_path = repo / 'scripts/run_bootstrap_all.py'
        runner_source = runner_path.read_bytes()
        self.assertEqual(hashlib.sha1(b'blob %d\0' % len(runner_source) + runner_source).hexdigest(), '1edc04c8fef035bd4f6efa3fd75fa4187774adf5')
        namespace = {'__name__': 'f08_actual_runner_fixture', '__file__': str(runner_path)}
        sys.path.insert(0, str(repo / 'scripts'))
        self.addCleanup(lambda: sys.path.remove(str(repo / 'scripts')))
        exec(compile(runner_source, str(runner_path), 'exec'), namespace)
        self.assertEqual(namespace['prepare_source'].__module__, 'bootstrap_all.adapters.registry')
        import bootstrap_all.adapters.registry as registry
        self.assertEqual(Path(registry.__file__).resolve(), repo / 'scripts/bootstrap_all/adapters/registry.py')
        calls = []
        fixture_password = 'f08-fixture-password-only'
        original_argv = sys.argv
        original_which = shutil.which
        original_isatty = sys.stdin.isatty
        compose_up = ['docker', 'compose', '-f', str((self.root / 'infra/docker-compose.yml').resolve()), 'up', '-d', 'neo4j', 'api', 'frontend']
        ingest_shell = 'cd /workspace/etl && uv run bracc-etl run --source example --neo4j-uri bolt://neo4j:7687 --neo4j-user neo4j --neo4j-password "$NEO4J_PASSWORD" --neo4j-database neo4j --data-dir ../data --linking-tier full'
        ingest_argv = ['docker', 'compose', '-f', str((self.root / 'infra/docker-compose.yml').resolve()), '--profile', 'etl', 'run', '--rm', '-e', f'NEO4J_PASSWORD={fixture_password}', 'etl', 'bash', '-lc', ingest_shell]

        def inert_external_call(command, *, cwd, env=None, timeout=None):
            requested = list(command)
            actual_cwd = str(cwd)
            if actual_cwd != str(self.root):
                raise AssertionError(f'unexpected cwd: {actual_cwd}')
            if requested == compose_up:
                calls.append({'argv': requested, 'cwd': actual_cwd, 'accepted': 'compose-up'})
                return subprocess.CompletedProcess(command, 0, '', '')
            if requested == ingest_argv:
                calls.append({'argv': requested, 'cwd': actual_cwd, 'accepted': 'etl-ingest'})
                return subprocess.CompletedProcess(command, 0, '', '')
            raise AssertionError(f'unexpected external argv refused before effect: {requested!r}')

        namespace['run_cmd'] = inert_external_call
        namespace['wait_for_neo4j'] = lambda *args, **kwargs: True
        namespace['wait_for_api'] = lambda *args, **kwargs: True
        namespace['parse_dotenv'] = lambda path: {}
        with mock.patch.dict(os.environ, {'PATH': '/usr/bin:/bin', 'NEO4J_PASSWORD': fixture_password, 'PYTHONDONTWRITEBYTECODE': '1', 'TMPDIR': str(self.root)}, clear=True), mock.patch.object(sys, 'argv', ['run_bootstrap_all.py', '--repo-root', str(self.root), '--doctor']), mock.patch.object(shutil, 'which', return_value='/inert/docker'), mock.patch.object(sys.stdin, 'isatty', wraps=sys.stdin.isatty):
            doctor_input = self.root / 'data/example/input.csv'
            doctor_input.unlink()
            doctor_input.mkdir()
            try:
                with mock.patch.object(namespace['sys'], 'stdout', io.StringIO()), mock.patch.object(namespace['sys'], 'stderr', io.StringIO()):
                    doctor_returncode = namespace['main']()
                self.assertEqual(doctor_returncode, 1)
                self.assertEqual(calls, [])
                input_path = doctor_input
                for expect_ingestion in (False, True):
                    if input_path.is_dir():
                        input_path.rmdir()
                    elif input_path.exists():
                        input_path.unlink()
                    if expect_ingestion:
                        input_path.write_text('id\n1\n')
                    else:
                        input_path.mkdir()
                    sys.argv = ['run_bootstrap_all.py', '--repo-root', str(self.root), '--no-reset']
                    calls.clear()
                    with mock.patch.object(namespace['sys'], 'stdout', io.StringIO()), mock.patch.object(namespace['sys'], 'stderr', io.StringIO()):
                        returncode = namespace['main']()
                    self.assertEqual(returncode, 0)
                    kinds = [call['accepted'] for call in calls]
                    self.assertEqual(kinds, ['compose-up', 'etl-ingest'] if expect_ingestion else ['compose-up'])
                    self.assertEqual([call['argv'] for call in calls if call['accepted'] == 'compose-up'], [compose_up])
                    self.assertEqual([call['argv'] for call in calls if call['accepted'] == 'etl-ingest'], [ingest_argv] if expect_ingestion else [])
                    self.assertTrue(all(call['cwd'] == str(self.root) for call in calls))
                    if expect_ingestion:
                        with self.assertRaisesRegex(AssertionError, 'unexpected external argv refused before effect'):
                            inert_external_call(['docker', 'compose', '-f', compose_up[3], 'down'], cwd=self.root)
                        with self.assertRaisesRegex(AssertionError, 'unexpected external argv refused before effect'):
                            inert_external_call(ingest_argv[:-1] + ['unexpected shell args'], cwd=self.root)
            finally:
                doctor_input = self.root / 'data/example/input.csv'
                if doctor_input.is_dir():
                    doctor_input.rmdir()
                elif doctor_input.exists():
                    doctor_input.unlink()
                self.assertEqual(sys.argv, ['run_bootstrap_all.py', '--repo-root', str(self.root), '--no-reset'])
        self.assertIs(sys.argv, original_argv)
        self.assertIs(shutil.which, original_which)
        self.assertEqual(sys.stdin.isatty, original_isatty)

    def test_normal_run_validates_bad_command_before_docker(self):
        repo = Path(__file__).resolve().parents[3]
        (self.root / 'docs').mkdir()
        (self.root / 'docs/source_registry_br_v1.csv').write_text('pipeline_id,in_universe_v1,implementation_state\nexample,true,implemented\n')
        source = self.contract['sources'][0]
        source['acquisition_mode'] = 'script_download'
        source['download_commands'] = ['python3 /workspace/scripts/missing.py']
        (self.root / 'config/bootstrap_all_contract.yml').write_text(json.dumps(self.contract))
        empty_path = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: empty_path.rmdir())
        env = {**os.environ, 'PATH': str(empty_path)}
        result = subprocess.run([sys.executable, str(repo / 'scripts/run_bootstrap_all.py'), '--repo-root', str(self.root)], capture_output=True, text=True, check=False, env=env)
        self.assertEqual(result.returncode, 1)
        self.assertIn('unsupported or missing download command', result.stderr)
        self.assertNotIn('docker is required', result.stderr)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['config', 'data', 'docs', 'etl'])


if __name__ == '__main__':
    unittest.main()
