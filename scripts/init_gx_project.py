"""
Initialize Great Expectations file-based project

This script creates a persistent Great Expectations project directory.
"""

import os
import great_expectations as gx

# Create gx directory if it doesn't exist
gx_dir = "gx"
if not os.path.exists(gx_dir):
    os.makedirs(gx_dir)
    print(f"Created {gx_dir} directory")

# Create file data context
try:
    context = gx.get_context(mode="file")
    print("Great Expectations file context created successfully!")
    print(f"Context root: {context.root_directory}")
except Exception as e:
    print(f"Error creating context: {e}")
    import traceback
    traceback.print_exc()
