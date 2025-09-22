from __future__ import annotations

"""
File handling utilities: downloading files and generating thumbnails.
"""
import os
import logging
import ffmpeg
from typing import Optional

logger = logging.getLogger(__name__)


def generate_video_thumbnail(video_path: str) -> Optional[str]:
    """Generate a .webp thumbnail for a given video in uploads/thumbnail.

    Returns the thumbnail path or None on failure.
    """
    try:
        if not os.path.exists(video_path):
            logger.error(f"Video file not found for thumbnail generation: {video_path}")
            return None

        thumbnail_dir = os.path.join('uploads', 'thumbnail')
        os.makedirs(thumbnail_dir, exist_ok=True)

        base_filename = os.path.splitext(os.path.basename(video_path))[0]
        thumbnail_filename = f"{base_filename}.webp"
        thumbnail_path = os.path.join(thumbnail_dir, thumbnail_filename)

        if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
            return thumbnail_path

        (
            ffmpeg
            .input(video_path, ss='00:00:05')
            .output(thumbnail_path, vframes=1, **{
                'c:v': 'libwebp',
                'quality': '80',
                'lossless': '0'
            })
            .overwrite_output()
            .run(quiet=True, capture_stdout=True)
        )

        if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
            return thumbnail_path
        return None
    except ffmpeg.Error as e:
        try:
            stderr_text = e.stderr.decode() if e.stderr else 'Unknown error'
        except Exception:
            stderr_text = 'Unknown error'
        logger.error(f"FFmpeg error generating thumbnail for {video_path}: {stderr_text}")
        return None
    except Exception as e:
        logger.error(f"Error generating thumbnail for {video_path}: {e}")
        return None


def generate_thumbnails_for_all_videos() -> None:
    """Generate thumbnails for all videos in uploads/ missing a thumbnail."""
    try:
        uploads_dir = 'uploads'
        if not os.path.exists(uploads_dir):
            logger.warning("Uploads directory not found")
            return

        video_extensions = ('.mp4', '.webm', '.mov', '.avi', '.mkv')
        for filename in os.listdir(uploads_dir):
            if filename.lower().endswith(video_extensions):
                video_path = os.path.join(uploads_dir, filename)
                base_filename = os.path.splitext(filename)[0]
                thumbnail_path = os.path.join('uploads', 'thumbnail', f"{base_filename}.webp")
                if not os.path.exists(thumbnail_path):
                    generate_video_thumbnail(video_path)
    except Exception as e:
        logger.error(f"Error in batch thumbnail generation: {e}")


def generate_video_thumbnail_to_dir(video_path: str, thumbnails_dir: str) -> Optional[str]:
    """Generate a .webp thumbnail for a given video into a specific directory.

    Returns the thumbnail path or None on failure.
    """
    try:
        if not os.path.exists(video_path):
            logger.error(f"Video file not found for thumbnail generation: {video_path}")
            return None

        os.makedirs(thumbnails_dir, exist_ok=True)

        base_filename = os.path.splitext(os.path.basename(video_path))[0]
        thumbnail_filename = f"{base_filename}.webp"
        thumbnail_path = os.path.join(thumbnails_dir, thumbnail_filename)

        if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
            return thumbnail_path

        (
            ffmpeg
            .input(video_path, ss='00:00:05')
            .output(thumbnail_path, vframes=1, **{
                'c:v': 'libwebp',
                'quality': '80',
                'lossless': '0'
            })
            .overwrite_output()
            .run(quiet=True, capture_stdout=True)
        )

        if os.path.exists(thumbnail_path) and os.path.getsize(thumbnail_path) > 0:
            return thumbnail_path
        return None
    except ffmpeg.Error as e:
        try:
            stderr_text = e.stderr.decode() if e.stderr else 'Unknown error'
        except Exception:
            stderr_text = 'Unknown error'
        logger.error(f"FFmpeg error generating thumbnail for {video_path}: {stderr_text}")
        return None
    except Exception as e:
        logger.error(f"Error generating thumbnail for {video_path}: {e}")
        return None