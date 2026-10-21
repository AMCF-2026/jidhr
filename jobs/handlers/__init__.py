"""Every job handler, imported so each one registers itself with the runner.

Add a new job type by adding a module here and importing it below.
"""

from jobs.handlers import mirror_refresh  # noqa: F401
from jobs.handlers import registrations_sync  # noqa: F401
from jobs.handlers import daf_sync  # noqa: F401
