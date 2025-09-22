#!/usr/bin/env python3
"""
Standalone script to generate thumbnails for all video files.
This script can be run independently to process existing videos.
"""

import os
import sys
import logging

# Add the project directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.processor import generate_thumbnails_for_all_videos

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to generate thumbnails."""
    logger.info("🎞️ Starting thumbnail generation for all videos...")
    
    try:
        generate_thumbnails_for_all_videos()
        logger.info("✅ Thumbnail generation completed successfully!")
    except Exception as e:
        logger.error(f"❌ Error during thumbnail generation: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
