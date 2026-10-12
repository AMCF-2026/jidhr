"""Every inline <script> in templates/ must be parseable JavaScript.

V5.67 shipped this in templates/chat.html's sendMessage() catch block:

    addMessage(
        '⚠️ **The reply did not come back** — but the server '
        'may still be working, and it may already have '
        ...
        'assistant');

Adjacent string literals with no `+`. That is implicit concatenation in
Python and a SyntaxError in JavaScript, so the ENTIRE <script> failed to
parse: sendMessage and handleKeyDown were never defined, and both Enter and
the ➤ button did nothing. The chat was dead for everyone, and the Python
suite was green the whole time — nothing had ever parsed the templates.

Two layers here, deliberately:

  * A real JS parser, when one is on the machine (node, deno or bun). It
    catches every syntax error, not just this shape.
  * A pure-Python check for adjacent string literals, which always runs.
    A guard that silently skips where no JS runtime is installed is a
    guard that would not have caught the bug it exists for.
"""

import os
import re
import shutil
import subprocess

import pytest

TEMPLATES = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "templates")

# A real parser, in order of preference. `node --check` is the canonical
# one; deno and bun are accepted so the guard still bites on a machine
# without node (this one, as it happens).
CHECKERS = (
    ("node", lambda path: ["node", "--check", path]),
    ("deno", lambda path: ["deno", "check", "--no-lock", path]),
    ("bun", lambda path: ["bun", "build", "--no-bundle", path]),
)

_SCRIPT_RE = re.compile(
    r"<script(?P<attrs>[^>]*)>(?P<body>.*?)</script>",
    re.DOTALL | re.IGNORECASE)


def template_files():
    if not os.path.isdir(TEMPLATES):
        return []
    return sorted(os.path.join(TEMPLATES, name)
                  for name in os.listdir(TEMPLATES)
                  if name.endswith((".html", ".htm")))


def inline_scripts(path):
    """[(line_number, source)] for every inline <script> in one template.

    A <script src=...> has no body to parse, and a non-JS type (a JSON-LD
    block, an x-template) is not JavaScript, so neither is checked.
    """
    text = open(path, encoding="utf-8").read()
    out = []
    for match in _SCRIPT_RE.finditer(text):
        attrs = match.group("attrs") or ""
        if "src=" in attrs.lower():
            continue
        kind = re.search(r"""type\s*=\s*["']?([^"'\s>]+)""", attrs,
                         re.IGNORECASE)
        if kind and kind.group(1).lower() not in (
                "text/javascript", "application/javascript", "module"):
            continue
        body = match.group("body")
        if not body.strip():
            continue
        line = text.count("\n", 0, match.start("body")) + 1
        out.append((line, body))
    return out


def find_checker():
    for name, build in CHECKERS:
        if shutil.which(name):
            return name, build
    return None, None


# ---------------------------------------------------------------------------
# The parser pass
# ---------------------------------------------------------------------------

def test_a_template_with_a_script_exists():
    """If this fails the rest of the file is vacuously green."""
    scripts = [s for path in template_files() for s in inline_scripts(path)]
    assert scripts, "no inline <script> found — the guard would prove nothing"


def test_every_inline_script_parses(tmp_path):
    """The canonical check: hand each <script> body to a real JS parser."""
    name, build = find_checker()
    if name is None:
        pytest.skip("no node, deno or bun on this machine — "
                    "test_no_adjacent_string_literals still covers the "
                    "V5.67 regression")

    failures = []
    for path in template_files():
        for line, body in inline_scripts(path):
            scratch = tmp_path / f"{os.path.basename(path)}.{line}.js"
            scratch.write_text(body, encoding="utf-8")
            done = subprocess.run(build(str(scratch)), capture_output=True,
                                  text=True, timeout=120)
            if done.returncode != 0:
                detail = (done.stderr or done.stdout or "").strip()
                failures.append(
                    f"{os.path.relpath(path)}: <script> at line {line} "
                    f"does not parse ({name}):\n{detail[:900]}")

    assert not failures, "\n\n".join(failures)


# ---------------------------------------------------------------------------
# The regression itself, checked without needing a JS runtime
# ---------------------------------------------------------------------------

# A line whose last non-space character closes a string literal, where the
# next line opens one. In Python that is concatenation; in JS it is a
# SyntaxError. Lines ending in an operator, comma, bracket or backtick are
# fine, which is what makes this specific rather than noisy.
_ENDS_WITH_STRING = re.compile(r"""(?:'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")\s*$""")
_STARTS_WITH_STRING = re.compile(r"""^\s*(?:'|")""")


def adjacent_string_literals(body):
    """[(offset, line)] for each Python-style implicit concatenation."""
    hits = []
    lines = body.split("\n")
    for i, line in enumerate(lines[:-1]):
        code = line.split("//", 1)[0] if "://" not in line else line
        if not _ENDS_WITH_STRING.search(code.rstrip()):
            continue
        following = lines[i + 1]
        if _STARTS_WITH_STRING.match(following):
            hits.append((i + 1, following.strip()))
    return hits


def test_no_adjacent_string_literals_in_any_template():
    """The exact V5.67 shape, caught with no JS runtime required."""
    failures = []
    for path in template_files():
        for line, body in inline_scripts(path):
            for offset, text in adjacent_string_literals(body):
                failures.append(
                    f"{os.path.relpath(path)}:{line + offset} — a string "
                    f"literal follows a string literal with no '+'. That is "
                    f"Python concatenation and a JS SyntaxError:\n"
                    f"    {text[:120]}")
    assert not failures, "\n\n".join(failures)


def test_the_detector_catches_the_v567_block():
    """The detector is only worth having if it fires on the real thing."""
    broken = """
            addMessage(
                '⚠️ **The reply did not come back** — but the server '
                'may still be working, and it may already have '
                'finished.',
                'assistant');
"""
    assert adjacent_string_literals(broken), \
        "the detector would not have caught the bug it exists for"


def test_the_detector_passes_the_fixed_block():
    fixed = """
            addMessage(
                '⚠️ **The reply did not come back** — but the server ' +
                'may still be working, and it may already have ' +
                'finished.',
                'assistant');
"""
    assert adjacent_string_literals(fixed) == []


@pytest.mark.parametrize("snippet", [
    "const a = ['one',\n 'two'];",                  # array elements
    "fn('one',\n   'two');",                        # two arguments
    "const o = {a: 'one',\n b: 'two'};",            # object values
    "const s = 'one' +\n 'two';",                   # explicit concatenation
    "const t = `one\n two`;",                       # template literal
])
def test_the_detector_does_not_fire_on_valid_javascript(snippet):
    """A noisy guard gets deleted, and then the bug comes back."""
    assert adjacent_string_literals(snippet) == [], snippet


# ---------------------------------------------------------------------------
# The functions the broken script took down
# ---------------------------------------------------------------------------

def test_chat_still_defines_its_handlers():
    """sendMessage and handleKeyDown were undefined for every visitor while
    the suite was green. Enter and the ➤ button both do nothing without
    them."""
    page = open(os.path.join(TEMPLATES, "chat.html"), encoding="utf-8").read()

    for name in ("function sendMessage", "function handleKeyDown",
                 "function addMessage"):
        assert name in page, name
