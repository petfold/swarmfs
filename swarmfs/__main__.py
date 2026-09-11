"""``python -m swarmfs`` — same as the ``swarmfs`` console script."""

import sys

from .cli import main

sys.exit(main())
