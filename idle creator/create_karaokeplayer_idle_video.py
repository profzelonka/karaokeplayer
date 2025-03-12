#!/usr/bin/env python3
import subprocess
import os
import glob

def get_video_duration(input_video):
    """Gets the duration of the input video using FFmpeg."""
    command = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        input_video
    ]
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return float(result.stdout.strip())
    except Exception as e:
        print(f"Error getting video duration: {e}")
        return None

def process_videos(input_folder="makeidle"):
    """
    Processes all .mov and .mp4 videos in the input folder,
    overlays the karaoke logo (if available), and exports them to the script directory.
    """
    # Find all .mov and .mp4 files
    video_files = glob.glob(os.path.join(input_folder, "*.mov")) + glob.glob(os.path.join(input_folder, "*.mp4"))
    
    if not video_files:
        print("Error: No .mov or .mp4 files found in 'makeidle' folder.")
        return

    logo_file = os.path.join(input_folder, "karaokeplayerlogo.png")
    logo_exists = os.path.exists(logo_file)

    if not logo_exists:
        print("\n⚠️ Warning: 'karaokeplayerlogo.png' was not found in the 'makeidle' folder.")
        user_choice = input("Would you like to continue processing videos without the logo? (yes/no): ").strip().lower()
        if user_choice != "yes":
            print("Exiting...")
            return
        print("Proceeding with conversion without logo overlay...\n")

    for input_video in video_files:
        # Extract the filename without extension
        base_name = os.path.splitext(os.path.basename(input_video))[0]
        output_filename = f"{base_name}.mp4"  # Output in the script's directory

        # Get video duration
        duration = get_video_duration(input_video)
        if duration is None:
            print(f"Skipping {input_video}: Unable to determine duration.")
            continue

        # Base FFmpeg command
        command = [
            "ffmpeg",
            "-y",  # Overwrite output if it exists
            "-i", input_video,  # Use the existing video as the background
        ]

        # If the logo exists, add overlay filters
        if logo_exists:
            command += [
                "-loop", "1",
                "-i", logo_file,
                "-filter_complex",
                (
                    "[0:v]scale=1920:1080:force_original_aspect_ratio=decrease,"
                    "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black[bg];"
                    "[1:v]scale=-1:400,unsharp=5:5:1.0[logo];"
                    "[bg][logo]overlay=(W-w)/2:(H-h)/2[final]"
                ),
                "-map", "[final]",
            ]
        else:
            # Just scale the video to 1080p without overlay
            command += [
                "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black",
            ]

        # Final encoding settings
        command += [
            "-c:v", "libx264",
            "-crf", "18",         # Lower CRF for high-quality output
            "-preset", "slow",    # Slow preset for better compression/quality
            "-pix_fmt", "yuv420p",
            "-t", str(duration),  # Match the length of the original video
            output_filename
        ]

        print(f"Processing: {input_video} → {output_filename}")
        subprocess.run(command, check=True)
        print(f"✅ Finished: {output_filename}")

def main():
    print("Starting processing for all videos in 'makeidle' folder...")
    process_videos()
    print("All videos processed successfully.")

if __name__ == "__main__":
    main()
