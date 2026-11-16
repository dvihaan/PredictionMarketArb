"""Tests for the code embedded in infra/stack.yaml: the CloudFront password function (run in Node,
skipped if Node isn't installed) and the Athena query Lambda (run with a fake Athena client).

    .venv/bin/python -m unittest discover -s tests
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATE = os.path.join(ROOT, "infra", "stack.yaml")


class CfnLoader(yaml.SafeLoader):
    """Reads CloudFormation YAML, keeping each !Tag's argument as plain data."""


def _any_tag(loader, tag_suffix, node):
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_mapping(node)


CfnLoader.add_multi_constructor("!", _any_tag)


def resources():
    with open(TEMPLATE, encoding="utf-8") as f:
        return yaml.load(f, Loader=CfnLoader)["Resources"]


PASSWORD = "s3cret:pw"            # contains a colon on purpose
HASH = hashlib.sha256(PASSWORD.encode()).hexdigest()

NODE_TESTS = r"""
const handler = require('./auth.js');
const H = process.argv[2];
const b64 = s => Buffer.from(s).toString('base64');
const req = (uri, headers = {}, cookies = {}) => ({request: {uri, headers, cookies}});
const cases = [
  ['login page is public', req('/login.html'), r => r.uri === '/login.html'],
  ['root without auth redirects to login', req('/'), r => r.statusCode === 302 && r.headers.location.value === '/login.html'],
  ['html without auth redirects to login', req('/index.html'), r => r.statusCode === 302],
  ['data without auth is 401 with a Basic challenge', req('/curated/manifest.json'),
   r => r.statusCode === 401 && /Basic/.test(r.headers['www-authenticate'].value)],
  ['cookie with the hash passes; / serves index.html', req('/', {}, {pma_auth: {value: H}}), r => r.uri === '/index.html'],
  ['wrong cookie fails', req('/x.parquet', {}, {pma_auth: {value: 'nope'}}), r => r.statusCode === 401],
  ['Bearer password passes and the header is removed', req('/api/query', {authorization: {value: 'Bearer s3cret:pw'}}),
   r => r.uri === '/api/query' && !r.headers.authorization],
  ['Basic any-user:password passes (password with a colon)',
   req('/curated/a.parquet', {authorization: {value: 'Basic ' + b64('vihaan:s3cret:pw')}}), r => r.uri === '/curated/a.parquet'],
  ['Basic wrong password fails', req('/curated/a.parquet', {authorization: {value: 'Basic ' + b64('vihaan:wrong')}}),
   r => r.statusCode === 401],
];
let failed = [];
for (const [name, ev, check] of cases) { if (!check(handler(ev))) failed.push(name); }
console.log(JSON.stringify(failed));
"""


class AuthFunctionTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js not installed")
    def test_password_rules(self):
        code = resources()["AuthFunction"]["Properties"]["FunctionCode"].replace("${WebPasswordHash}", HASH)
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "auth.js"), "w", encoding="utf-8") as f:
                f.write(code + "\nmodule.exports = handler;\n")
            with open(os.path.join(d, "run.js"), "w", encoding="utf-8") as f:
                f.write(NODE_TESTS)
            out = subprocess.run(["node", "run.js", HASH], cwd=d, capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(out.stdout), [], out.stderr)


FORMAT_TESTS = r"""
const fs = require('fs');
const html = fs.readFileSync(process.argv[2], 'utf8');
const src = html.slice(html.indexOf('// formatValue-begin'), html.indexOf('// formatValue-end'));
const formatValue = new Function(src + '; return formatValue;')();
// An Arrow DecimalBigNum (HUGEINT/DECIMAL), as apache-arrow 17 behaves: String() gives the digits,
// toJSON() gives them wrapped in quote characters (the cause of the quoted suspect_rows bug).
const dec = (s) => ({toString: () => s, toJSON: () => JSON.stringify(s)});
const cases = [
  ['date from epoch ms (Nov 9 bug report)', formatValue(1794268800000, 'Date32<DAY>'), '2026-11-10'],
  ['HUGEINT sum (the escaped-quotes bug report)', formatValue(dec('606'), 'Decimal[38e0]'), '606'],
  ['HUGEINT zero', formatValue(dec('0'), 'Decimal[38e0]'), '0'],
  ['decimal with scale 2', formatValue(dec('-12345'), 'Decimal[18e+2]'), '-123.45'],
  ['small decimal with scale 3', formatValue(dec('5'), 'Decimal[18e+3]'), '0.005'],
  ['BIGINT', formatValue(12345678901234567890n, 'Int64'), '12345678901234567890'],
  ['timestamp in microseconds', formatValue(1794858900000000, 'Timestamp<MICROSECOND>'), '2026-11-16 19:55:00.000'],
  ['timestamp in milliseconds', formatValue(1794858900000, 'Timestamp<MILLISECOND>'), '2026-11-16 19:55:00.000'],
  ['list of structs', formatValue({toJSON: () => [{price: 0.4, size: 10}]}, 'List<Struct<price, size>>'), '[{"price":0.4,"size":10}]'],
  ['null', formatValue(null, 'Int32'), ''],
  ['plain number and string', formatValue(0.72, 'Float64') + '|' + formatValue('kalshi', 'Utf8'), '0.72|kalshi'],
];
const failed = cases.filter(([, got, want]) => got !== want).map(([n, got, want]) => `${n}: got ${got} want ${want}`);
console.log(JSON.stringify(failed));
"""


class QueryPageTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js not installed")
    def test_value_formatting(self):
        page = os.path.join(ROOT, "infra", "site", "index.html")
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "run.js"), "w", encoding="utf-8") as f:
                f.write(FORMAT_TESTS)
            out = subprocess.run(["node", "run.js", page], cwd=d, capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(out.stdout), [], out.stderr)


class FakeAthena:
    class exceptions:
        class InvalidRequestException(Exception):
            pass

    def __init__(self, running_states=()):
        self.running_states = list(running_states)
        self.started = []

    def list_query_executions(self, **kw):
        return {"QueryExecutionIds": [f"id-{i}" for i in range(len(self.running_states))]}

    def batch_get_query_execution(self, QueryExecutionIds):
        return {"QueryExecutions": [{"Status": {"State": s}} for s in self.running_states]}

    def start_query_execution(self, **kw):
        self.started.append(kw)
        return {"QueryExecutionId": "11111111-2222-3333-4444-555555555555"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"WorkGroup": "wg", "Status": {"State": "SUCCEEDED"},
                                   "Statistics": {"DataScannedInBytes": 123, "TotalExecutionTimeInMillis": 456}}}

    def get_query_results(self, QueryExecutionId, MaxResults):
        return {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": [{"Name": "n"}]},
                              "Rows": [{"Data": [{"VarCharValue": "n"}]}, {"Data": [{"VarCharValue": "7"}]}]}}


class QueryFunctionTest(unittest.TestCase):
    def load(self, athena):
        code = resources()["QueryFunction"]["Properties"]["Code"]["ZipFile"]
        fake_boto3 = types.SimpleNamespace(client=lambda name: athena)
        saved = sys.modules.get("boto3")
        sys.modules["boto3"] = fake_boto3
        os.environ.update(WORKGROUP="wg", DATABASE="db", MAX_RUNNING="2")
        try:
            mod = types.ModuleType("query_lambda")
            exec(compile(code, "query_lambda", "exec"), mod.__dict__)
        finally:
            if saved is None:
                sys.modules.pop("boto3", None)
            else:
                sys.modules["boto3"] = saved
        return mod

    def call(self, mod, path, **q):
        r = mod.handler({"rawPath": path, "queryStringParameters": q}, None)
        return r["statusCode"], json.loads(r["body"])

    def test_starts_read_only_query(self):
        athena = FakeAthena()
        code, body = self.call(self.load(athena), "/api/query", sql="SELECT count(*) FROM books;")
        self.assertEqual(code, 200)
        self.assertEqual(athena.started[0]["QueryString"], "SELECT count(*) FROM books")
        self.assertEqual(athena.started[0]["WorkGroup"], "wg")

    def test_rejects_writes_and_multiple_statements(self):
        mod = self.load(FakeAthena())
        for sql in ("DROP TABLE books", "CREATE TABLE x AS SELECT 1", "SELECT 1; DROP TABLE books", ""):
            self.assertEqual(self.call(mod, "/api/query", sql=sql)[0], 400, sql)

    def test_limits_concurrent_queries(self):
        code, _ = self.call(self.load(FakeAthena(["RUNNING", "QUEUED"])), "/api/query", sql="SELECT 1")
        self.assertEqual(code, 429)

    def test_status_and_results(self):
        mod = self.load(FakeAthena())
        qid = "11111111-2222-3333-4444-555555555555"
        code, body = self.call(mod, "/api/status", id=qid)
        self.assertEqual((code, body["state"], body["csv"]), (200, "SUCCEEDED", f"/athena-results/{qid}.csv"))
        code, body = self.call(mod, "/api/results", id=qid)
        self.assertEqual(body, {"columns": ["n"], "rows": [["7"]], "truncated": False})
        self.assertEqual(self.call(mod, "/api/status", id="../etc")[0], 400)


if __name__ == "__main__":
    unittest.main()
