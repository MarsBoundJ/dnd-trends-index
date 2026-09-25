"""Abstention in the BackerKit ingest route: NULL is not zero.

    python3 scripts/test_bouncer_abstention.py

WHY THIS EXISTS. The route used to write

    'funding_usd':   float(r.get('funding_usd') or 0.0),
    'backers_count': int(r.get('backers_count') or 0),
    'days_remaining': int(r.get('days_remaining') or 0),

so a project whose funding could not be read became a project that raised
nothing, and no downstream check could tell the two apart. The harvester's
own abstention was pointless: it sent null and Python's `or` re-zeroed it on
arrival.

Verified 2026-09-25 against INFORMATION_SCHEMA before changing this --
funding_usd, backers_count and days_remaining are all NULLABLE on
commercial_data.backerkit_projects, so a null cannot trigger the SILENT row
drop that insert_rows_json(skip_invalid_rows=True) performs on a REQUIRED
column. That risk is the only reason the coercion survived this long, and it
turned out not to apply.

HOW IT READS THE SOURCE. bouncer/main.py cannot be imported here -- it pulls
functions_framework, vertexai and google.cloud at module scope. So the helper
block is SLICED OUT by marker and exec'd, exactly as the JavaScript suites
slice the harvester normalisers. Nothing is copied: if the block moves, this
fails loudly rather than testing a stale duplicate.
"""

import io
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = io.open(os.path.join(HERE, '..', 'bouncer', 'main.py'), encoding='utf-8').read()

START = '# Abstention helpers.'
END = 'def safe_int(val, default=0):'

a, b = SRC.find(START), SRC.find(END)
if a < 0 or b < 0 or b <= a:
    raise SystemExit('Could not find the abstention helper block in bouncer/main.py. '
                     'If it moved, update the markers -- do not delete this test.')

ns = {}
exec(compile(SRC[a:b], 'bouncer_helpers', 'exec'), ns)
f_or_none = ns['_float_or_none']
i_or_none = ns['_int_or_none']
t_or_none = ns['_text_or_none']

_pass = _fail = 0


def check(name, actual, expected):
    global _pass, _fail
    ok = actual == expected and type(actual) is type(expected)
    if ok:
        _pass += 1
    else:
        _fail += 1
    print(('  ok   ' if ok else '  FAIL ') + name +
          ('' if ok else '\n         expected %r (%s), got %r (%s)'
           % (expected, type(expected).__name__, actual, type(actual).__name__)))


print('\nunmeasured values become None, never 0:')
check('None stays None', f_or_none(None), None)
check('empty string is not zero', f_or_none(''), None)
check('unparseable text is not zero', f_or_none('abc'), None)
check('None stays None (int)', i_or_none(None), None)
check('empty string is not zero (int)', i_or_none(''), None)
check('unparseable text is not zero (int)', i_or_none('not-a-number'), None)

print('\nbut a REAL zero survives -- that is the whole distinction:')
check('0.0 is a measurement', f_or_none(0.0), 0.0)
check('0 is a measurement', i_or_none(0), 0)
check('"0" is a measurement', f_or_none('0'), 0.0)
check('"0" is a measurement (int)', i_or_none('0'), 0)

print('\nordinary values pass through:')
check('a float', f_or_none(2107889.0), 2107889.0)
check('a numeric string', f_or_none('2107889'), 2107889.0)
check('an int', i_or_none(8805), 8805)
check('a numeric string (int)', i_or_none('8805'), 8805)
check('a negative day count is kept, not clamped', i_or_none(-42), -42)

print('\nvalues BigQuery cannot store as FLOAT64 are refused:')
# NaN and the infinities would be rejected by the streaming insert, and NaN in
# particular compares unequal to itself, so a naive float() guard lets it past.
check('NaN is not a number', f_or_none(float('nan')), None)
check('+inf is refused', f_or_none(float('inf')), None)
check('-inf is refused', f_or_none(float('-inf')), None)

print('\ntext abstains on empty and truncates on long:')
check('None stays None', t_or_none(None, 500), None)
check('empty becomes None, not ""', t_or_none('', 500), None)
check('whitespace-only becomes None', t_or_none('   ', 500), None)
check('a real name survives', t_or_none('Free League Publishing', 500), 'Free League Publishing')
check('surrounding space is trimmed', t_or_none('  Wet Ink Games  ', 500), 'Wet Ink Games')
check('a long value is truncated', t_or_none('x' * 40, 16), 'x' * 16)
check('a currency marker fits in 16', t_or_none('NZ$', 16), 'NZ$')

print('\nthe route actually uses them:')


def route():
    i = SRC.find("elif path == 'system/backerkit/ingest-projects':")
    j = SRC.find("elif path ==", i + 10)
    if i < 0:
        raise SystemExit('backerkit ingest route not found')
    return SRC[i:j if j > i else len(SRC)]


R = route()
for field, helper in [('funding_usd', '_float_or_none'),
                      ('backers_count', '_int_or_none'),
                      ('days_remaining', '_int_or_none'),
                      ('funding_amount', '_float_or_none'),
                      ('goal_amount', '_float_or_none')]:
    check("%s uses %s" % (field, helper),
          ("'%s': %s(r.get('%s'))" % (field, helper, field)) in R, True)
check("funding_currency is captured with its unit",
      "'funding_currency': _text_or_none(r.get('funding_currency'), 16)" in R, True)

# The dict is EXPLICIT. A field the harvester sends faithfully is dropped
# without a word if it is not listed here -- the same silent-discard family as
# ignore_unknown_values against a missing column. Every field the V3 card
# harvester emits has to appear.
for field, helper in [('blurb', '_text_or_none'),
                      ('category', '_text_or_none'),
                      ('status', '_text_or_none'),
                      ('trending_rank', '_int_or_none')]:
    check("%s is forwarded, not dropped" % field,
          ("'%s': %s(r.get('%s')" % (field, helper, field)) in R, True)

print('\nand the coercions that caused this are gone:')
for gone in ["or 0.0)", "or 0)"]:
    check('no %r left in the route' % gone, gone in R, False)
check('creator no longer defaults to an empty string',
      "str(r.get('creator', ''))" in R, False)

print('\nthe measured reason is recorded where the next reader will look:')
PROSE = re.sub(r'\s+', ' ', re.sub(r'^\s*#\s?', '', R, flags=re.M))
check('the NULLABLE verification is written down', 'are all NULLABLE on this table' in PROSE, True)
check('the multi-currency measurement is written down',
      'BackerKit displays amounts in the project' in PROSE, True)
check('and that conversion must not happen at ingest',
      'never at ingest' in PROSE, True)

print('\n%d passed, %d failed\n' % (_pass, _fail))
sys.exit(1 if _fail else 0)
