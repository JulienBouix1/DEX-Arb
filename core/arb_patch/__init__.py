# Core arb_patch exports - only actively used modules
from .executor import dual_ioc
from .flat import flatten_symmetry, flat_all

# Note: allocator.py and guard.py were legacy modules (never called) - moved to legacy/
